from fastapi import FastAPI, Depends, HTTPException, UploadFile
from sqlmodel import Session, SQLModel, create_engine, Field, text
from typing import Annotated, Optional, List, Dict
from datetime import datetime
import pandas as pd
import io


DATABASE_URL = (
    'mssql+pyodbc://localhost/PracticeAPI?driver=ODBC+Driver+17+for+SQL+Server'
)

engine = create_engine(DATABASE_URL)



def get_session():
    with Session(engine) as session:
        yield session


session_dep = Annotated[Session, Depends(get_session)]


class Jobs(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job : str


class Employees(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name : str | None
    datetime : datetime | None
    department_id : int | None
    job_id : int | None

class Departments(SQLModel, table=True):
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
    """
    Executes a SQL query to return the number of employees hired per quarter (Q1–Q4)
    in the year 2021, grouped by department and job title.
    Each result row shows how many people were hired for a specific job in a 
    specific department in each quarter of 2021.
    """
    sql = """
        SELECT 
            deps.department,
            jobs.job,
            SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN DATEPART(QUARTER, emp.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM 
            employees as emp
        LEFT JOIN departments as deps
            ON deps.id = emp.department_id
        LEFT JOIN jobs
            ON jobs.id = emp.job_id
        WHERE 
            YEAR(emp.datetime) = 2021
            AND emp.department_id IS NOT NULL
        GROUP BY 
            deps.department,
            jobs.job
        ORDER BY 
            deps.department,
            jobs.job;
    """
    result = session.exec(text(sql))
    rows = result.fetchall()
    column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in rows]
    return data


@app.get('/hired/above-average-2021', response_model=List[Dict])
def hired_above_average(session: session_dep):
    """
    Executes a SQL query to return all departments whose number of hires
    in the year 2021 is greater than the average number of hires per department 
    during that year.
    """
    sql = """
        SELECT 
            department_id as id,
            deps.department, 
            COUNT(emp.id) AS hired
        FROM employees AS emp
        LEFT JOIN departments AS deps
            ON emp.department_id = deps.id
        WHERE YEAR(emp.datetime) = 2021
        GROUP BY emp.department_id, deps.department
        HAVING COUNT(emp.id) > (
            SELECT COUNT(id) / COUNT(DISTINCT department_id) 
            FROM employees 
            WHERE YEAR(datetime) = 2021
        )
        ORDER BY COUNT(emp.id) DESC;
    """

    result = session.exec(text(sql))
    rows = result.fetchall()
    column_names = result.keys()

    return [dict(zip(column_names, row)) for row in rows]



@app.post('/upload_departments/')
def upload_departments_csv(file: UploadFile, session: session_dep):
    """
    Upload department data from a CSV file and insert it into the database.
    The expected CSV should not have a header. 
    The rows are inserted into the 'Departments' table. 
    If a department with the same 'id' already exists in the database, 
    the entry is skipped.

    The CSV is expected to contain rows with the following columns:
        - id (int): Unique department ID
        - department (str): Department name
    """

    contents = file.file.read()
    df = pd.read_csv(io.StringIO(contents.decode()), header=None, 
                     names=['id', 'department'])

    departments = []
    for index, row in df.iterrows():
        departments.append(Departments(
            id = row['id'],
            department = row['department']
        ))

    added = 0
    for deptartment in departments:
        if not session.get(Departments, deptartment.id):
            session.add(deptartment)
            added +=1
    session.commit()

    if not added:
        return {'message': f'Successfully uploaded {len(departments)} records.'}
    else:
        return {'message': f'Uploaded {added} records. Repeated IDs were skipped.'}



@app.post('/upload_jobs/')
def upload_jobs_csv(file: UploadFile, session: session_dep):
    """
    Upload department data from a CSV file and insert it into the database.
    The expected CSV should not have a header. 
    The rows are inserted into the 'Jobs' table. 
    If a department with the same 'id' already exists in the database, 
    the entry is skipped.

    The CSV is expected to contain rows with the following columns:
        - id (int): Unique job ID
        - job (str): Job name
    """

    contents = file.file.read()
    df = pd.read_csv(io.StringIO(contents.decode()), header=None, 
                     names=['id', 'job'])

    jobs = []
    for index, row in df.iterrows():
        jobs.append(Jobs(
            id = row['id'],
            job = row['job']
        ))
    
    added = 0
    for job in jobs:
        if not session.get(Jobs, job.id):
            session.add(job)
            added +=1
    session.commit()

    if not added:
        return {'message': f'Successfully uploaded {len(jobs)} records.'}
    else:
        return {'message': f'Uploaded {added} records. Repeated IDs were skipped.'}


@app.post('/upload_hired/')
def upload_hired(file: UploadFile, session: session_dep):
    """
    Upload hired employee data from a CSV file and insert into the database in batches.
    The expected CSV should not have a header. 
    The rows are inserted into the 'Employee' table in batches of 1000 rows.
    If a department with the same 'id' already exists in the database, 
    the entry is skipped.

    The CSV is expected to contain rows with the following columns:
        - id (int): Unique Employee ID
        - name (str): Employee name
        - datetime (str): Hiring date and time (ISO format)
        - department_id (int): ID of the department
        - job_id (int): ID of the job
    """

    contents = file.file.read() 
    df = pd.read_csv(io.StringIO(contents.decode()), header=None,
                         names=['id', 'name', 'datetime', 'department_id', 'job_id'])

    # Converting to datetime like this could cause errors.
    df['datetime'] = pd.to_datetime(df['datetime'])
    # Need to manually convert some NAs into None so they can be uploaded as NULLS to the DB.
    df = df.astype(object).where(pd.notnull(df), None)
    
    batch_size = 1000
    total_inserted = 0
    added = 0
    try:
        for start in range(0, len(df), batch_size):
            chunk = df.iloc[start:start + batch_size]
            hired_employees = []
            for index, row in chunk.iterrows():
                hired_employees.append(Employees(
                    id=row['id'],
                    name=row['name'],
                    datetime=row['datetime'],
                    department_id=row['department_id'],
                    job_id=row['job_id']
                ))

            for hired_employee in hired_employees:
                if not session.get(Employees, hired_employee.id):
                    session.add(hired_employee)
                    added += 1
            session.commit()

            total_inserted += len(hired_employees)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Batch insert failed: {str(e)}')

    if added <= total_inserted:
        return {'message': f'Successfully uploaded {total_inserted} records in batches of 1000.'}
    else:
        return {'message': f'Uploaded {added} records. Repeated IDs were skipped.'}

