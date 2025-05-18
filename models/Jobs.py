from sqlmodel import SQLModel, Field
from typing import Optional


class Jobs(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job : str
