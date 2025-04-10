from fastapi import FastAPI, HTTPException, Depends, Request, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from rcc_app import models, schemas, crud
from .database import engine, Base, get_db, get_db_access
from datetime import datetime,timedelta
import pyodbc
import socket
import sys
import os

from openpyxl import load_workbook
from typing import List
from collections import defaultdict


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

Base.metadata.create_all(bind=engine)

app = FastAPI()


# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/login/")
def login(login_data: schemas.Login, request: Request, db: Session = Depends(get_db)):
    # Get the IP address of the client
    client_ip = request.client.host

    # Query user by email
    user = db.query(models.User).filter(models.User.usremail == login_data.email).first()
    # Handle case where the user does not exist
    if not user:
        # Log the failed login attempt
        log_attempt = models.LoginAttempt(
            usrid=None,  # Set to None when the user does not exist
            ipaddr=client_ip,
            attemptat=datetime.now(),
            success=False,
            reason="User not found"
        )
        db.add(log_attempt)
        db.commit()
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Validate the password
    if user.__getattribute__("password") != login_data.password:  # Use __getattribute__ to access the field
        # Log the failed login attempt
        log_attempt = models.LoginAttempt(
            usrid=user.usrid,
            ipaddr=client_ip,
            attemptat=datetime.now(),
            success=False,
            reason="Invalid password"
        )
        db.add(log_attempt)
        db.commit()
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Log the successful login attempt
    log_attempt = models.LoginAttempt(
        usrid=user.usrid,
        ipaddr=client_ip,
        attemptat=datetime.now(),
        success=True,
        reason="Successful Login"
    )
    db.add(log_attempt)
    db.commit()

    return {"message": "Login successful", "user": {"id": user.usrid, "name": user.usrnamedisplay}}


# fetching users from postgre
@app.get("/user/{user_id}")
def get_user_info(user_id: int, db: Session = Depends(get_db)):
    """
    User information retrieval endpoint
    """
    user = db.query(models.User).filter(models.User.usrid == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": user.usrid, "name": user.usrnamedisplay}


# reading offline wtgs from microsoft access DB
@app.get("/offline_wtgs")
def get_offline_wtgs(db: pyodbc.Connection = Depends(get_db_access)):
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT 
            e.dtTS1DownBegin, 
            f.facABBR, 
            a.astName, 
            r.rtnName, 
            rr.rsnName, 
            n.evntntNote,
            ROUND((IIF(e.dtTS7EventFinish IS NOT NULL, e.dtTS7EventFinish, Now()) - e.dtTS1DownBegin) * 24, 2) AS DowntimeHrs
        FROM 
            ((((tblEvent AS e
            INNER JOIN tblFacility AS f ON e.facID = f.facID)
            INNER JOIN tblAsset AS a ON e.astID = a.astID)
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
            INNER JOIN tblReason as rr ON e.rsnID = rr.rsnID)
            INNER JOIN tblEventNotes as n ON e.evntID = n.evntID
        WHERE 
            e.dtTS7EventFinish IS NULL;
        """
        )  # Modify with your actual query
    rows = cursor.fetchall()

    # Extract the column names dynamically from the cursor description
    columns = [column[0] for column in cursor.description]
    
    # Create a list of dictionaries with column names as keys
    data = [dict(zip(columns, row)) for row in rows]

    return {"offlineWtgsDataSet": data}

# reading service events from microsoft access DB
@app.get("/get_services")
async def get_services(
    startdate: str = Query(..., description="Start date in format YYYY-MM-DD"),
    enddate: str = Query(..., description="End date in format YYYY-MM-DD"),
    db: pyodbc.Connection = Depends(get_db_access)):
    cursor = db.cursor()
    try:
        start_dt = datetime.strptime(startdate, "%Y-%m-%d")
        end_dt = datetime.strptime(enddate, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    cursor.execute(
        """
        SELECT 
            e.dtTS1DownBegin, 
            f.facABBR, 
            a.astName, 
            r.rtnName, 
            rr.rsnName, 
            n.evntntNote
        FROM 
            ((((tblEvent AS e
            INNER JOIN tblFacility AS f ON e.facID = f.facID)
            INNER JOIN tblAsset AS a ON e.astID = a.astID)
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
            INNER JOIN tblReason as rr ON e.rsnID = rr.rsnID)
            LEFT JOIN tblEventNotes as n ON e.evntID = n.evntID
        WHERE 
            e.dtTS1DownBegin BETWEEN ? AND ?
            AND r.rtnName NOT IN ('Fault', 'IDF Outage', 'Other', 'IDF Fault')
            AND rr.rsnName <> 'Communication loss'
        """,
        (start_dt, end_dt)
        )  # Modify with your actual query
    rows = cursor.fetchall()

    # Extract the column names dynamically from the cursor description
    columns = [column[0] for column in cursor.description]
    
    # Create a list of dictionaries with column names as keys
    data = [dict(zip(columns, row)) for row in rows]

    return {"servicesDataSet": data}


# reading fault events from microsoft access DB
@app.get("/get_faults")
async def get_faults(
    startdate: str = Query(..., description="Start date in format YYYY-MM-DD"),
    enddate: str = Query(..., description="End date in format YYYY-MM-DD"),
    db: pyodbc.Connection = Depends(get_db_access)):
    cursor = db.cursor()
    try:
        start_dt = datetime.strptime(startdate, "%Y-%m-%d")
        end_dt = datetime.strptime(enddate, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    cursor.execute(
        """
        SELECT 
            f.facABBR, 
            a.astName, 
            r.rtnName, 
            fa.fltCode,
            fa.fltDesc,
            e.dtTS1DownBegin,
            e.dtTS7DownFinish,
            ROUND((IIF(e.dtTS7EventFinish IS NOT NULL, e.dtTS7EventFinish, Now()) - e.dtTS1DownBegin) * 24, 2) AS DowntimeHrs
        FROM 
            (((((tblEvent AS e
            INNER JOIN tblFacility AS f ON e.facID = f.facID)
            INNER JOIN tblAsset AS a ON e.astID = a.astID)
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
            INNER JOIN tblReason as rr ON e.rsnID = rr.rsnID)
            LEFT JOIN tblEventNotes as n ON e.evntID = n.evntID)
            INNER JOIN tblFaultCode as fa ON e.fltID = fa.fltID
        WHERE 
            e.fltID IS NOT NULL AND
            e.dtTS1DownBegin BETWEEN ? AND ?
        """,
        (start_dt, end_dt)
        )  # Modify with your actual query
    rows = cursor.fetchall()

    # Extract the column names dynamically from the cursor description
    columns = [column[0] for column in cursor.description]
    
    # Create a list of dictionaries with column names as keys
    data = [dict(zip(columns, row)) for row in rows]

    return {"faultsDataSet": data}


# reading from excel (raw data 2025)
@app.get("/read-excel/", response_model=List[models.ExcelRow])
async def read_excel():
    excel_file_path = r"C:\Users\gwarcc\goldwindaustralia\Service SharePoint - Service Technical Library\22 RCC\RCC\18. RCC Reporting\01 Yearly Raw Data\2025\RCC Benefit Raw Data 2025.xlsm"

    wb = load_workbook(excel_file_path)
    sheet = wb.active

    headers = [
        "Date", "Wind Farm", "WTG", "WTG Type", "WTG Type 2", "Wind Speed", "Category", 
        "Reason", "Alarm Code", "Alarm Description", "Downtime", "Stop Time", "Maint Time", 
        "Start Time", "Remarks", "RCC Notified Time", "Before or After RCC Control", 
        "Weekend Day/Hour", "Day/Night", "Reset Level", "RCC Notified time (min)", 
        "Reset By", "Response Time", "Before reset by Site/ After Reset by RCC", 
        "IDF Fault Time Saving"
    ]

    # Read rows from the Excel sheet and store them in a list of dictionaries
    rows = []
    for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row, min_col=1, max_col=len(headers)):
        row_data = {headers[i]: row[i].value for i in range(len(headers))}
        rows.append(row_data)

    return rows
    

@app.get("/summary_stoppages")
def get_summary_stoppages(
    startdate: str = Query(..., description="Start date in YYYY-MM-DD"),
    enddate: str = Query(..., description="End date in YYYY-MM-DD"),
    db: pyodbc.Connection = Depends(get_db_access)
):
    cursor = db.cursor()

    try:
        start_dt = datetime.strptime(startdate, "%Y-%m-%d")
        end_dt = datetime.strptime(enddate, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    # Query Access database
    cursor.execute(
    """
    SELECT 
        f.facABBR AS windfarm, 
        r.rtnName AS category
    FROM 
        ((tblEvent AS e
        INNER JOIN tblFacility AS f ON e.facID = f.facID)
        INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
    WHERE 
        e.dtTS1DownBegin BETWEEN ? AND ?
    """,
    (start_dt, end_dt)
)


    rows = cursor.fetchall()
    summary = defaultdict(lambda: defaultdict(int))


    for row in rows:
        wf = row.windfarm
        cat = row.category.strip().lower() if row.category else ""

        summary[wf]["Total Stops"] += 1

        if cat == "schedule service":
            summary[wf]["Scheduled Services"] += 1
        elif cat in ["fault", "idf fault"]:
            summary[wf]["Faults"] += 1
        else:
            summary[wf]["Non Scheduled Services"] += 1
        
    result = []
    for wf, types in summary.items():
        for typ, count in types.items():
            result.append({"windfarm": wf, "type": typ, "count": count})

    return result



@app.get("/stoppage_legend")
def get_stoppage_legend(
    startdate: str = Query(..., description="Start date in YYYY-MM-DD"),
    enddate: str = Query(..., description="End date in YYYY-MM-DD"),
    db: pyodbc.Connection = Depends(get_db_access)
):
    cursor = db.cursor()

    try:
        start_dt = datetime.strptime(startdate, "%Y-%m-%d")
        end_dt = datetime.strptime(enddate, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    cursor.execute(
        """
        SELECT 
            r.rtnName AS category,
            rr.rsnName AS rsnName
        FROM 
            ((tblEvent AS e
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
            INNER JOIN tblReason AS rr ON e.rsnID = rr.rsnID)
        WHERE 
            e.dtTS1DownBegin BETWEEN ? AND ?
        """,
        (start_dt, end_dt)
    )

    rows = cursor.fetchall()
    legend_summary = defaultdict(lambda: defaultdict(int))

    for row in rows:
        cat = row.category.strip().lower() if row.category else ""
        rsn = row.rsnName.strip() if row.rsnName else "Unknown"

        if cat == "schedule service":
            typ = "Scheduled Services"
        elif cat in ["fault", "idf fault"]:
            typ = "Faults"
        else:
            typ = "Non Scheduled Services"

        legend_summary[typ][rsn] += 1

    result = []
    for typ, reasons in legend_summary.items():
        for rsn, count in reasons.items():
            result.append({
                "type": typ,
                "rsnName": rsn,
                "count": count
            })

    return result


@app.get("/stoppage_headings")
def get_stoppage_legend(
    startdate: str = Query(..., description="Start date in YYYY-MM-DD"),
    enddate: str = Query(..., description="End date in YYYY-MM-DD"),
    db: pyodbc.Connection = Depends(get_db_access)
):
    cursor = db.cursor()

    try:
        start_dt = datetime.strptime(startdate, "%Y-%m-%d")
        end_dt = datetime.strptime(enddate, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    cursor.execute(
        """
        SELECT 
            COUNT(IIf(r.rtnName NOT LIKE '*Communication*' 
                AND r.rtnName NOT LIKE '*IDF Fault*' 
                AND r.rtnName NOT LIKE '*IDF Outage*', 1, NULL)) AS total_stoppages,
            COUNT(IIf(r.rtnName IN ('Schedule Service', 'Scheduled - Adhoc', 'Scheduled Inspections'), 1, NULL)) AS scheduled_stoppages,
            COUNT(IIf(r.rtnName NOT IN ('Schedule Outage', 'Schedule Service', 'Scheduled - Adhoc', 
                'Scheduled Inspections', 'Fault', 'IDF Fault', 'IDF Outage', 'Communication'), 1, NULL)) AS non_scheduled_stoppages,
            COUNT(IIf(r.rtnName IN ('Fault','IDF Fault'), 1, NULL)) AS fault_stoppages,
            ROUND(AVG(IIf(e.dtTS7DownFinish IS NOT NULL AND e.dtTS3MaintBegin IS NOT NULL, 
                DateDiff('s', e.dtTS3MaintBegin, e.dtTS7DownFinish) / 3600.0, 0)), 2) AS avg_maint,
            ROUND(AVG(IIf(e.dtTS7DownFinish IS NOT NULL AND e.dtTS1EventBegin IS NOT NULL, 
                DateDiff('s', e.dtTS1EventBegin, e.dtTS7DownFinish) / 3600.0, 0)), 2) AS avg_down
        FROM 
            (tblEvent AS e
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
        WHERE 
            e.dtTS1DownBegin BETWEEN ? AND ?
        """,
        (start_dt, end_dt)
    )

    rows = cursor.fetchall()

    # Assuming the query will return one row with the count result
    if rows:
        total_stoppages = rows[0][0]
        scheduled_stoppages = rows[0][1]
        non_scheduled_stoppages = rows[0][2]
        fault_stoppages = rows[0][3]
        avg_maint = rows[0][4]
        avg_down = rows[0][5]
    else:
        total_stoppages = 0
        scheduled_stoppages = 0
        non_scheduled_stoppages = 0
        fault_stoppages = 0
        avg_maint = 0
        avg_down = 0

    # Structure the result into a dictionary or list format
    result = {
        "total_stoppages": total_stoppages,
        "scheduled_stoppages": scheduled_stoppages,
        "non_scheduled_stoppages": non_scheduled_stoppages,
        "fault_stoppages": fault_stoppages,
        "avg_maint": avg_maint,
        "avg_down": avg_down
    }

    return result

@app.get("/offline_headings")
def get_stoppage_legend(
    db: pyodbc.Connection = Depends(get_db_access)
):
    cursor = db.cursor()

    cursor.execute(
        """
        SELECT 
            COUNT(IIf(e.dtTS7EventFinish IS NULL, 1, NULL)) AS total_offline
        FROM 
            (tblEvent AS e
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID)
        """
    )

    rows = cursor.fetchall()

    # Assuming the query will return one row with the count result
    if rows:
        total_offline = rows[0][0]
    else:
        total_offline = 0

    # Structure the result into a dictionary or list format
    result = {
        "total_offline": total_offline
    }

    return result

@app.get("/services_details")
def get_services_details(
    startdate: str = Query(..., description="Start date in YYYY-MM-DD"),
    enddate: str = Query(..., description="End date in YYYY-MM-DD"),
    db: pyodbc.Connection = Depends(get_db_access)
):
    cursor = db.cursor()

    try:
        start_dt = datetime.strptime(startdate, "%Y-%m-%d")
        end_dt = datetime.strptime(enddate, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    cursor.execute(
        """
        SELECT 
            COUNT(IIf(r.rtnName NOT IN ('Fault', 'IDF Fault', 'IDF Outage'), 1, NULL)) AS total_services,

            COUNT(IIf(r.rtnName IN ('Schedule Service', 'Scheduled - Adhoc', 'Scheduled Inspections', 'Scheduled Outage'), 1, NULL)) AS scheduled_services,

            COUNT(IIf(
                r.rtnName NOT IN (
                    'Fault', 'IDF Fault', 'IDF Outage', 
                    'Schedule Service', 'Scheduled - Adhoc', 'Scheduled Inspections', 'Scheduled Outage'
                ), 1, NULL)) AS non_scheduled_services,

            ROUND(
                SUM(IIf(r.rtnName NOT IN ('Fault', 'IDF Fault', 'IDF Outage') AND e.dtTS3MaintBegin IS NOT NULL AND e.dtTS7DownFinish IS NOT NULL,
                    DateDiff('s', e.dtTS3MaintBegin, e.dtTS7DownFinish), 0)) / 
                COUNT(IIf(r.rtnName NOT IN ('Fault', 'IDF Fault', 'IDF Outage') AND e.dtTS3MaintBegin IS NOT NULL AND e.dtTS7DownFinish IS NOT NULL, 1, NULL)) / 3600.0
            , 2) AS avg_maint,
            
            ROUND(
                SUM(IIf(r.rtnName NOT IN ('Fault', 'IDF Fault', 'IDF Outage') AND e.dtTS1EventBegin IS NOT NULL AND e.dtTS7DownFinish IS NOT NULL,
                    DateDiff('s', e.dtTS1EventBegin, e.dtTS7DownFinish), 0)) / 
                COUNT(IIf(r.rtnName NOT IN ('Fault', 'IDF Fault', 'IDF Outage') AND e.dtTS1EventBegin IS NOT NULL AND e.dtTS7DownFinish IS NOT NULL, 1, NULL)) / 3600.0
            , 2) AS avg_down_time
            
            

        FROM 
            tblEvent AS e
            INNER JOIN tblRationale AS r ON e.rtnID = r.rtnID

        WHERE 
            e.dtTS1DownBegin BETWEEN ? AND ?
        """,
        (start_dt, end_dt)
    )

    row = cursor.fetchone()

    if row:
        total_services = row[0]
        scheduled_services = row[1]
        non_scheduled_services = row[2]
        avg_service_time = row[3]
        avg_down_time = row[4]
    else:
        total_services = 0
        scheduled_services = 0
        non_scheduled_services = 0
        avg_service_time = 0.0
        avg_down_time = 0.0

    return {
        "total_services": total_services,
        "scheduled_services": scheduled_services,
        "non_scheduled_services": non_scheduled_services,
        "avg_service_time": avg_service_time,
        "avg_down_time": avg_down_time
    }
