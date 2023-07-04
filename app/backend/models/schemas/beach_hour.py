from typing import Optional, List

from pydantic import BaseModel

# Shared properties
class BeachHourBase(BaseModel):
    local: str = None
    boaHora: str = None
    isDay: bool = None
    temperatura: str = None
    pp: str = None
    uv: str = None
    hora: str = None

    
# BeachHour that has a list of BeachHourBase items
class BeachHour:
    __root__: List[BeachHourBase] = None