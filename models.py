from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, Boolean, Text
from .database import Base
from sqlalchemy.orm import relationship
from datetime import datetime

class User(Base):
    __tablename__ = "tblusers"

    usrid = Column(Integer, primary_key=True, index=True)
    usrnamefirst = Column(String, index=True)
    usrnamelast = Column(String, index=True)
    usrnamedisplay = Column(String)
    password = Column(String)  # Renamed from 'pass'
    usremail = Column(String, unique=True, index=True)

class LoginAttempt(Base):
    __tablename__ = 'tblloginattempts'

    laid = Column(Integer, primary_key=True, index=True)
    usrid = Column(Integer)
    ipaddr = Column(String, nullable=True)
    attemptat = Column(TIMESTAMP, default=datetime.utcnow)
    success = Column(Boolean, nullable=False)
    reason = Column(Text, nullable=True)
