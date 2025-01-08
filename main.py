from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from . import models, schemas, crud
from .database import engine, Base, get_db

Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.post("/examples/", response_model=schemas.Example)
def create_example(example: schemas.ExampleCreate, db: Session = Depends(get_db)):
    return crud.create_example(db, example)

@app.get("/examples/", response_model=list[schemas.Example])
def read_examples(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    return crud.get_examples(db, skip=skip, limit=limit)
