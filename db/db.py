from typing import Annotated
from fastapi import Depends
from sqlmodel import SQLModel, Session, create_engine

DB_NAME = "PracticeAPI"

DATABASE_URL = f'mssql+pyodbc://localhost/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(DATABASE_URL)


def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session


session_dep = Annotated[Session, Depends(get_session)]


