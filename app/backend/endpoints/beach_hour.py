from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Query

from app.backend.models.kafka_consumers.generic import GenericConsumer
from app.backend.models.schemas.beach_hour import BeachHourBase

import json

router = APIRouter()

CITIES = json.loads(open('assets/cities.json', 'r').read())

@router.get("/all_beach_hour/",  tags=["Beach Hour"])
async def get_all_beach_hour() -> list[BeachHourBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic="beachHour").get()
    print(messages)
   
    return messages

@router.get("/beach_hour/",  tags=["Beach Hour"])
async def get_all_beach_hour_city(city: str = Query(..., description="City to check beach hour", enum=list(CITIES.keys()))) -> list[BeachHourBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic='beachHour').get(city=city)
    print(messages)
   
    return messages