from sqlalchemy.orm import Session
from . import models, schemas

def create_example(db: Session, example: schemas.ExampleCreate):
    db_example = models.ExampleTable(**example.dict())
    db.add(db_example)
    db.commit()
    db.refresh(db_example)
    return db_example

def get_examples(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.ExampleTable).offset(skip).limit(limit).all()
