from sqlmodel import SQLModel, Field
from datetime import datetime
from typing import Optional



class Employees(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name : Optional[str]
    datetime : Optional[datetime]
    department_id : Optional[int]
    job_id : Optional[int]
