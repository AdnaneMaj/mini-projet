from fastapi import FastAPI
from routers import base

app = FastAPI()

app.include_router(base.base_router)