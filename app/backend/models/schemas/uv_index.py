from typing import Optional, List

from pydantic import BaseModel

# Shared properties
class UvIndexBase(BaseModel):
    local: str = None
    hora: str = None
    uv_index: float = None
    
# BeachHour that has a list of BeachHourBase items
class UvIndex:
    __root__: List[UvIndexBase] = None