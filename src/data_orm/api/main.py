from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from data_orm.api.persons.routers.person import router as person_router

# Initialisation de l'application FastAPI
app = FastAPI(
    title="Financial loan model enhanced",
    description="This API allow to update database used for training model",
    version="1.0.0"
)

# CORS pour Angular
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    person_router,
    prefix="/api/v1"
)