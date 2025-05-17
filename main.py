from fastapi import FastAPI, Depends
from sqlmodel import Session, SQLModel, create_engine, Field, text
from typing import Annotated, Optional, List, Dict
from datetime import datetime


DATABASE_URL = (
    'mssql+pyodbc://localhost/PracticeAPI'
    '?driver=ODBC+Driver+17+for+SQL+Server'
)

engine = create_engine(DATABASE_URL)



def get_session():
    with Session(engine) as session:
        yield session


session_dep = Annotated[Session, Depends(get_session)]


class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job : str


class Employees(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name : str | None
    datetime : datetime | None
    department_id : int | None
    job_id : int | None

class Department(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    department : str


class DepartmentsBase(SQLModel, table=True):
    id : int = Field(default=None, primary_key=True)
    department : str



app = FastAPI()


SQLModel.metadata.create_all(engine)

@app.get('/')
def root():
    return {'message': 'Hello World'}



@app.get('/departmentsbase/', response_model=list[DepartmentsBase])
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



@app.get('/hired/quarterly', response_model=List[Dict])
def get_hired_table(session: session_dep):
    sql = """
        SELECT 
            depsb.department,
            jobb.job,
            SUM(CASE WHEN DATEPART(QUARTER, heb.[datetime]) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN DATEPART(QUARTER, heb.[datetime]) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN DATEPART(QUARTER, heb.[datetime]) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN DATEPART(QUARTER, heb.[datetime]) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM 
            hired_employeesbase as heb
        LEFT JOIN [dbo].[departmentsbase] as depsb
            ON depsb.id = heb.department_id
        LEFT JOIN [dbo].[jobsbase] as jobb
            ON jobb.id = heb.job_id
        WHERE 
            YEAR(heb.[datetime]) = 2021
            AND heb.department_id IS NOT NULL
        GROUP BY 
            depsb.department,
            jobb.job
        ORDER BY 
            depsb.department,
            jobb.job;
    """
    result = session.exec(text(sql))
    rows = result.fetchall()
    column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in rows]
    return data


@app.get('/hired/above-average-2021', response_model=List[Dict])
def hired_above_average(session: session_dep):
    sql = """
        SELECT 
            department_id as id,
            depsb.department, 
            COUNT(empb.id) AS hired
        FROM [dbo].[hired_employeesbase] AS empb
        LEFT JOIN [dbo].[departmentsbase] AS depsb
            ON empb.department_id = depsb.id
        WHERE YEAR(empb.datetime) = 2021
        GROUP BY empb.department_id, depsb.department
        HAVING COUNT(empb.id) > (
            SELECT COUNT(id) / COUNT(DISTINCT department_id) 
            FROM [dbo].[hired_employeesbase] 
            WHERE YEAR(datetime) = 2021
        )
        ORDER BY COUNT(empb.id) DESC;
    """

    result = session.exec(text(sql))
    rows = result.fetchall()
    column_names = result.keys()

    return [dict(zip(column_names, row)) for row in rows]