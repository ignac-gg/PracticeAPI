from fastapi import APIRouter, UploadFile
from sqlmodel import text
import pandas as pd
import io

from db.db import session_dep
from models.Departments import Departments

router = APIRouter()


@router.post('/upload_departments/')
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



@router.get('/departmentsbase/', response_model=list[Departments])
def get_departments(session: session_dep):
    sql = """
        SELECT *
        FROM departments
    """

    result = session.exec(text(sql))
    rows = result.fetchall()
    column_names = result.keys()
    data = [dict(zip(column_names, row)) for row in rows]
    return data
