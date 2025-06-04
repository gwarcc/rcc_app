from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, Boolean, Text
from .database import Base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from pydantic import BaseModel

class User(Base):
    __tablename__ = "tblusers"

    usrid = Column(Integer, primary_key=True, index=True)
    usrnamefirst = Column(String, index=True)
    usrnamelast = Column(String, index=True)
    usrnamedisplay = Column(String)
    password = Column(String)  # Renamed from 'pass'
    usremail = Column(String, unique=True, index=True)
    usrrlid = Column(Integer, ForeignKey("tblusersrole.usrrlid"), default="3")  # User Roles for Access Control
    role = relationship("UserRole")

class UserRole(Base):
    __tablename__ = "tblusersrole"

    usrrlid = Column(Integer, primary_key=True, index=True)
    usrrlname = Column(String, index=True)
    usrrlvieworder = Column(Integer, index=True)
    rltypid = Column(Integer, index=True)
    
class LoginAttempt(Base):
    __tablename__ = 'tblloginattempts'

    laid = Column(Integer, primary_key=True, index=True)
    usrid = Column(Integer)
    ipaddr = Column(String, nullable=True)
    attemptat = Column(TIMESTAMP, default=datetime.utcnow)
    success = Column(Boolean, nullable=False)
    reason = Column(Text, nullable=True)

# set up model to read from excel file (raw data)
class ExcelRow(BaseModel):
    Date: datetime
    Wind_Farm: str
    WTG: str
    WTG_Type: str
    WTG_Type_2: str
    Wind_Speed: str
    Category: str
    Reason: str
    Alarm_Code: int
    Alarm_Description: str
    Downtime: str
    Stop_Time: datetime
    Maint_Time: datetime
    Start_Time: datetime
    Remarks: str
    RCC_Notified_Time: datetime
    Before_or_After_RCC_Control: str
    Weekend_Day_Hour: str
    Day_Night: str
    Reset_Level: str
    RCC_Notified_time_min: str
    Reset_By: str
    Response_Time: str
    Before_reset_by_Site_After_Reset_by_RCC: str
    IDF_Fault_Time_Saving: str