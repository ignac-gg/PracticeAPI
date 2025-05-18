from fastapi import FastAPI

from db.db import init_db

from routers.Departments import router as deps_router
from routers.Employees import router as emp_router
from routers.Jobs import router as jobs_router


app = FastAPI()

init_db()

@app.get('/')
def root():
    return {'message': 'Hello World'}


app.include_router(deps_router)
app.include_router(emp_router)
app.include_router(jobs_router)




