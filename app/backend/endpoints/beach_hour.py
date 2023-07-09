from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Query

from app.backend.models.kafka_consumers.generic import GenericConsumer
from app.backend.models.schemas.beach_hour import BeachHourBase

import json
from datetime import datetime

router = APIRouter()

CITIES = json.loads(open('assets/cities.json', 'r').read())

@router.get("/all_beach_hour/",  tags=["Beach Hour"])
async def get_all_beach_hour() -> list[BeachHourBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic='beachHourCopy').get()
    print(messages)

    # transform key hora string into datetime and then sort by datetime
    messages = sorted(messages, key=lambda x: datetime.strptime(x['hora'], '%Y-%m-%dT%H:%M'))
    return messages

@router.get("/beach_hour/",  tags=["Beach Hour"])
async def get_all_beach_hour_city(city: str = Query(..., description="City to check beach hour", enum=list(CITIES.keys()))) -> list[BeachHourBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic='beachHourCopy').get(city=city)
    print(messages)

    messages = sorted(messages, key=lambda x: datetime.strptime(x['hora'], '%Y-%m-%dT%H:%M'))
    return messages