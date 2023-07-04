from typing import Optional, List

from pydantic import BaseModel

# Shared properties
class BeachDayBase(BaseModel):
    local: str = None
    boaHora: str = None
    dia: str = None
    inicio: str = None 
    fim: str = None

# BeachDay that has a list of BeachDayBase items
class BeachDay:
    __root__: List[BeachDayBase] = None