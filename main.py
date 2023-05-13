from fastapi import FastAPI, Query, Response, status
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


@app.get("/beach_day_city", description="Get beach day events", tags=["Clients"])
async def beach_day(city: str = Query(..., description="City to check beach day", enum=list(CITIES.keys()))):
    client = BeachDayClient(city)
    messages = client.get_events()
    return messages

@app.get("/beach_day", description="Get beach day events", tags=["Clients"])
async def beach_day():
    messages = {city: [] for city in CITIES.keys()}
    for city in CITIES.keys():
        client = BeachDayClient(city)
        messages[city] = client.get_events()
    return messages

@app.get("/uv_index", description="Get UV index", tags=["Clients"], status_code=200)
async def uv_index(response: Response, city: str = Query(..., description="City to check UV index", enum=list(CITIES.keys()))):

    client = UnsafeUvClient(city)
    messages = client.get_messages()

    messages = sorted(messages, key=lambda x: x['hora'])
    if messages:
        return messages
    else:
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"message": "No data found for this date"}
    
@app.get("/uv_index_hour", description="Get UV index per hour", tags=["Clients"], status_code=200)
async def uv_index(response: Response, city: str = Query(..., description="City to check UV index", enum=list(CITIES.keys())), \
                   day: int = Query(..., description="Day to check UV index", ge=1, le=31), \
                   month: int = Query(..., description="Month to check UV index", ge=1, le=12), \
                   hour: int = Query(..., description="Hour to check UV index", ge=0, le=23), \
                   ):
    
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
@app.put("/weather_city", description="Update weather for specific city", tags=["Producers"], status_code=200)
async def weather(response: Response, city: str = Query(..., description="City to update weather", enum=list(CITIES.keys()))):
    producers = WeatherProducer()
    messages = producers.run(city)
    return {"Temperature events": messages[0], "Preciptation probability events": messages[1]}

@app.put("/weather", description="Update weather", tags=["Producers"], status_code=200)
async def weather(response: Response):
    producers = WeatherProducer()
    messages = {city: [] for city in CITIES.keys()}
    for city in CITIES.keys():
        messages[city] = producers.run(city)
    return messages

@app.put("/uv_index_city", description="Update UV index for specific city", tags=["Producers"], status_code=200)
async def uv_index(response: Response, city: str = Query(..., description="City to update UV index", enum=list(CITIES.keys()))):
    producers = UvProducer()
    messages = producers.run(city)
    return {"UV index events": messages}

@app.put("/uv_index", description="Update UV index", tags=["Producers"], status_code=200)
async def uv_index(response: Response):
    producers = UvProducer()
    messages = {city: [] for city in CITIES.keys()}
    for city in CITIES.keys():
        messages[city] = producers.run(city)
    return {"UV index events": messages}

#############################################################################################################
@app.put("/beach_hour", description="Update good beach hour", tags=["Client-Producers"], status_code=200)
async def beach_hour(response: Response):
    producers = BeachHourProducer()
    messages = producers.run()
    return {"Good beach hour events": messages}

@app.put("/beach_day", description="Update good beach day", tags=["Client-Producers"], status_code=200)
async def beach_day(response: Response):
    producers = BeachDayProducer()
    messages = producers.run()
    return {"Good beach day events": messages}

