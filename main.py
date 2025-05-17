from fastapi import FastAPI, Depends
from sqlmodel import Session, SQLModel, create_engine, Field, text
from typing import Annotated
from datetime import datetime


DATABASE_URL = (
    "mssql+pyodbc://localhost/PracticeAPI"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(DATABASE_URL)



def get_session():
    with Session(engine) as session:
        yield session


session_dep = Annotated[Session, Depends(get_session)]


class Job(SQLModel):
    id: int = Field(primary_key=True, index=True)
    job : str


class Hired(SQLModel):
    id : int = Field(default=None, primary_key=True)
    name : str | None
    datetime : datetime | None
    department_id : int | None
    job_id : int | None

class Department(SQLModel):
    id : int = Field(default=None, primary_key=True)
    department : str


class DepartmentsBase(SQLModel, table=True):
    id : int = Field(default=None, primary_key=True)
    department : str



app = FastAPI()

@app.get('/')
def root():
    return {"message": "Hello World"}



@app.get("/departmentsbase/", response_model=list[DepartmentsBase])
def get_departments(session: session_dep):
    sql = """
        SELECT *
        FROM departmentsbase as heb
    """

    result = session.exec(text(sql))
    rows = result.fetchall()
    column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in rows]
    return data