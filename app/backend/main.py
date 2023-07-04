from fastapi import FastAPI, Query, Response, status
from pydantic import BaseModel

import json
import datetime

from fastapi import APIRouter

from .endpoints import beach_day
from .endpoints import beach_hour
from .endpoints import uv_index


api_router = APIRouter()

api_router.include_router(beach_day.router)
api_router.include_router(beach_hour.router)
api_router.include_router(uv_index.router)


CITIES = json.loads(open('assets/cities.json', 'r').read())

app = FastAPI()

app.include_router(api_router)