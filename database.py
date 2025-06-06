from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pyodbc

# Postgre sql config
DATABASE_URL = "postgresql://postgres:Goldwind%40123@localhost:5432/rcc_dashboard"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


#miscrosoft access config
ACCESS_DB_PATH = r"c:\users\gwarcc\goldwindaustralia\service sharepoint - service technical library\22 rcc\rcc\22. rcc event tracker\database\rcc database v2.3.accdb"
ACCESS_CONNECTION_STRING = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + ACCESS_DB_PATH

# Microsoft Access config - Prod Stats
PROD_STATS_DB_PATH = r"C:\Users\gwarcc\goldwindaustralia\Service SharePoint - Service Technical Library\22 RCC\RCC\22. RCC Event Tracker\Database\RCC Prod Stats V1.0.accdb"
PROD_STATS_CONNECTION_STRING = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + PROD_STATS_DB_PATH
)

def get_access_connection():
    conn = pyodbc.connect(ACCESS_CONNECTION_STRING)
    return conn

def get_prod_stats_connection():
    return pyodbc.connect(PROD_STATS_CONNECTION_STRING)

Base = declarative_base()


# function to get database session for postgre
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# function to get database connection for microsoft access
def get_db_access():
    conn = get_access_connection()
    try:
        yield conn
    finally:
        conn.close()

# Access session - Prod Stats
def get_db_prod_stats():
    conn = get_prod_stats_connection()
    try:
        yield conn
    finally:
        conn.close()