from sqlalchemy import Column, Integer, String
from .database import Base

class User(Base):
    __tablename__ = "tblusers"

    usrid = Column(Integer, primary_key=True, index=True)
    usrnamefirst = Column(String, index=True)
    usrnamelast = Column(String, index=True)
    usrnamedisplay = Column(String)
    password = Column(String)  # Renamed from 'pass'
    usremail = Column(String, unique=True, index=True)
