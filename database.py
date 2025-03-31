from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pyodbc

# Postgre sql config
DATABASE_URL = "postgresql://postgres:Goldwind%40123@localhost:5432/rcc_dashboard"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


#miscrosoft access config
ACCESS_DB_PATH = r"C:\Users\gwarcc\Music\RCCEventTracker V2.2.008 Runtime.accdb"
ACCESS_CONNECTION_STRING = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + ACCESS_DB_PATH
def get_access_connection():
    conn = pyodbc.connect(ACCESS_CONNECTION_STRING)
    return conn


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

