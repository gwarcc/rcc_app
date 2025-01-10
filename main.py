from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from rcc_app import models, schemas, crud
from .database import engine, Base, get_db
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

Base.metadata.create_all(bind=engine)

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace '*' with specific origins for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/login/")
def login(login_data: schemas.Login, db: Session = Depends(get_db)):
    # Query user by email
    user = db.query(models.User).filter(models.User.usremail == login_data.email).first()
    if not user or user.__getattribute__("password") != login_data.password:  # Use __getattribute__ for 'pass'
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {"message": "Login successful", "user": {"id": user.usrid, "name": user.usrnamedisplay}}

@app.get("/user/{user_id}")
def get_user_info(user_id: int, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.usrid == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": user.usrid, "name": user.usrnamedisplay}
