from sqlalchemy import Column, Integer, String
from .database import Base

class ExampleTable(Base):
    __tablename__ = "example_table"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    value = Column(Integer)
