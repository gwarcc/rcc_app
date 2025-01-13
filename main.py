from fastapi import FastAPI, HTTPException, Depends, Request, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from rcc_app import models, schemas, crud
from .database import engine, Base, get_db
from datetime import datetime
import socket
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

@app.get("/user/{user_id}")
def get_user_info(user_id: int, db: Session = Depends(get_db)):
    """
    User information retrieval endpoint
    """
    user = db.query(models.User).filter(models.User.usrid == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": user.usrid, "name": user.usrnamedisplay}