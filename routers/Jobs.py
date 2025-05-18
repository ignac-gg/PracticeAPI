from fastapi import APIRouter, UploadFile

import io
import pandas as pd

from db.db import session_dep
from models.Jobs import Jobs

router = APIRouter()

@router.post('/upload_jobs/')
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


