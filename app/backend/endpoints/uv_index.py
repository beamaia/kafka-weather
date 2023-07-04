from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Query

from app.backend.models.kafka_consumers.generic import GenericConsumer
from app.backend.models.schemas.uv_index import UvIndexBase

import json

router = APIRouter()

CITIES = json.loads(open('assets/cities.json', 'r').read())

@router.get("/all_uv_index/",  tags=["UV Index"])
async def get_all_uv_index() -> List[UvIndexBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic="uvIndex").get()
    print(messages)
   
    return messages

@router.get("/uv_index/",  tags=["UV Index"])
async def get_all_uv_index_city(city: str = Query(..., description="City to check beach hour", enum=list(CITIES.keys()))) -> List[UvIndexBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic='uvIndex').get(city=city)
    print(messages)
   
    return messages