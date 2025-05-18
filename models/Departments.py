from sqlmodel import SQLModel, Field
from typing import Optional

class Departments(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    department : str


class DepartmentsBase(SQLModel, table=True):
    id : int = Field(default=None, primary_key=True)
    department : str