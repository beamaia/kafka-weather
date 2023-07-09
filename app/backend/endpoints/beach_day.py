from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Query

from app.backend.models.kafka_consumers.generic import GenericConsumer
from app.backend.models.schemas.beach_day import BeachDayBase

import json
from datetime import datetime

router = APIRouter()

CITIES = json.loads(open('assets/cities.json', 'r').read())

@router.get("/all_beach_day/",  tags=["Beach Day"])
async def get_all_beach_day() -> List[BeachDayBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic="beachDay").get()
    print(messages)
    # transform key inicio string to datetime and sort by datetime
    messages = sorted(messages, key=lambda x: datetime.strptime(x['inicio'], '%Y-%m-%dT%H:%M'))
    return messages

@router.get("/beach_day/",  tags=["Beach Day"])
async def get_all_beach_day_city(city: str = Query(..., description="City to check beach day", enum=list(CITIES.keys()))) -> List[BeachDayBase]:
    """
    Create new category.
    """
    messages = GenericConsumer(topic='beachDay').get(city=city)
    print(messages)

    # transform key inicio string to datetime and sort by datetime
    messages = sorted(messages, key=lambda x: datetime.strptime(x['inicio'], '%Y-%m-%dT%H:%M'))
   
    return messages



# @app.get("/beach_day_city", description="Get beach day events", tags=["Clients"])
# async def beach_day_city(city: str = Query(..., description="City to check beach day", enum=list(CITIES.keys()))):
#     """
#     Get beach day events for a specific city.

#     Parameters
#     ----------
#     city : str
#         City to check beach day.

#     Returns
#     -------
#     dict
#         Dictionary with beach day events for the given city.
#     """
#     client = BeachDayClient(city)
#     messages = client.get_messages()
#     return messages