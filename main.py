from fastapi import FastAPI, Query, Response, status
from pydantic import BaseModel

import json
import datetime

from client.beach_day_client import BeachDayClient
from client.unsafe_uv_client import UnsafeUvClient

from producers.weather_producer import WeatherProducer
from producers.uv_producer import UvProducer

from client_producers.beach_day_producer import BeachDayProducer
from client_producers.beach_hour_producer import BeachHourProducer

CITIES = json.loads(open('assets/cities.json', 'r').read())

app = FastAPI()

#############################################################################################################
"""
    Clients routes
"""

@app.get("/beach_day_city", description="Get beach day events", tags=["Clients"])
async def beach_day_city(city: str = Query(..., description="City to check beach day", enum=list(CITIES.keys()))):
    """
    Get beach day events for a specific city.

    Parameters
    ----------
    city : str
        City to check beach day.

    Returns
    -------
    dict
        Dictionary with beach day events for the given city.
    """
    client = BeachDayClient(city)
    messages = client.get_messages()
    return messages

@app.get("/beach_day", description="Get beach day events", tags=["Clients"])
async def beach_day():
    """
    Get beach day events for all cities.

    Parameters
    ----------
    None

    Returns
    -------
    dict
        Dictionary with beach day events for all cities.
    """
    messages = {city: [] for city in CITIES.keys()}
    for city in CITIES.keys():
        client = BeachDayClient(city)
        messages[city] = client.get_messages()
    return messages

@app.get("/uv_index", description="Get UV index", tags=["Clients"], status_code=200)
async def uv_index(response: Response, city: str = Query(..., description="City to check UV index", enum=list(CITIES.keys()))):
    """
    Get UV index for a specific city.

    Parameters
    ----------
    city : str
        City to check UV index.

    Parameters
    ----------
    city : str
        City to check UV index.

    Returns
    -------
    dict
        Dictionary with UV index for the given city.
    """
    client = UnsafeUvClient(city)
    messages = client.get_messages()

    messages = sorted(messages, key=lambda x: x['hora'])
    if messages:
        return messages
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "No data found for this date"}
    
@app.get("/uv_index_hour", description="Get UV index per hour", tags=["Clients"], status_code=200)
async def uv_index_hour(response: Response, city: str = Query(..., description="City to check UV index", enum=list(CITIES.keys())), \
                   day: int = Query(..., description="Day to check UV index", ge=1, le=31), \
                   month: int = Query(..., description="Month to check UV index", ge=1, le=12), \
                   hour: int = Query(..., description="Hour to check UV index", ge=0, le=23), \
                   ) -> dict:
    """
    Get UV index for a specific city, day, month and hour.

    Parameters
    ----------
    city : str
        City to check UV index.
    day : int
        Day to check UV index.
    month : int
        Month to check UV index.
    hour : int
        Hour to check UV index.

    Returns
    -------
    dict
        Dictionary with UV index for the given city, day, month and hour.
    """
    try:
        date = datetime.datetime(2023, month, day, hour)
    except ValueError:
        raise Exception("Invalid date")

    client = UnsafeUvClient(city, date)
    messages = client.get_events_by_date()
    if messages:
        return messages
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "No data found for this date"}

#############################################################################################################
"""	
    Producers routes
"""
@app.put("/weather_city", description="Update weather for specific city", tags=["Producers"], status_code=200)
async def weather_city(response: Response, city: str = Query(..., description="City to update weather", enum=list(CITIES.keys()))) -> dict:
    """
    Update weather for a specific city.

    Parameters
    ----------
    city : str
        City to update weather.

    Returns
    -------
    dict
        Dictionary with weather events for the given city.
    """
    producers = WeatherProducer()
    messages = producers.run(city)
    return {"Temperature events": messages[0], "Preciptation probability events": messages[1]}

@app.put("/weather", description="Update weather", tags=["Producers"], status_code=200)
async def weather(response: Response) -> dict:
    """
    Update weather for all cities.

    Parameters
    ----------
    None

    Returns
    -------
    dict
        Dictionary with weather events for all cities.
    """
    producers = WeatherProducer()
    messages = {city: [] for city in CITIES.keys()}
    for city in CITIES.keys():
        messages[city] = producers.run(city)
    return messages

@app.put("/uv_index_city", description="Update UV index for specific city", tags=["Producers"], status_code=200)
async def uv_index_city(response: Response, city: str = Query(..., description="City to update UV index", enum=list(CITIES.keys()))):
    """
    Update UV index for a specific city.

    Parameters
    ----------
    city : str
        City to update UV index.

    Returns
    -------
    dict
        Dictionary with UV index events for the given city.
    """
    producers = UvProducer()
    messages = producers.run(city)
    return {"UV index events": messages}

@app.put("/uv_index", description="Update UV index", tags=["Producers"], status_code=200)
async def uv_index(response: Response):
    """
    Update UV index for all cities.

    Parameters
    ----------
    None

    Returns
    -------
    dict
        Dictionary with UV index events for all cities.
    """
    producers = UvProducer()
    messages = {city: [] for city in CITIES.keys()}
    for city in CITIES.keys():
        messages[city] = producers.run(city)
    return {"UV index events": messages}

#############################################################################################################
"""	
    Client-Producers routes
"""

@app.put("/beach_hour", description="Update good beach hour", tags=["Client-Producers"], status_code=200)
async def beach_hour(response: Response):
    """
    Update good beach hour for all cities.

    Parameters
    ----------
    None

    Returns
    -------
    dict
        Dictionary with good beach hour events for all cities.  
    """	

    producers = BeachHourProducer()
    messages = producers.run()
    return {"Good beach hour events": messages}

@app.put("/beach_day", description="Update good beach day", tags=["Client-Producers"], status_code=200)
async def beach_day(response: Response):
    """
    Update good beach day for all cities.

    Parameters
    ----------
    None

    Returns
    -------
    dict
        Dictionary with good beach day events for all cities.
    """
    producers = BeachDayProducer()
    messages = producers.run()
    return {"Good beach day events": messages}

