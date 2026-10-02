from fastapi import FastAPI
from app.core.database import get_db
from app.models import Base
from app.api.route import api_router
from contextlib import asynccontextmanager
from app.core.database import engine
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware

@asynccontextmanager
async def lifespan(app: FastAPI):

    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

        print(" PostgreSQL connected successfully")
         # Create tables
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")

    yield

    await engine.dispose()


app = FastAPI(lifespan=lifespan)


origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}

app.include_router(api_router, prefix="/api/v1")

