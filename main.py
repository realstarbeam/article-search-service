import os
import logging
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from neo4j import GraphDatabase
from meilisearch_python_async import Client as MeiliClient
from meilisearch_python_async.errors import MeilisearchApiError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import asyncio
from contextlib import asynccontextmanager
from typing import List
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from logging_config import setup_logging
from neo4j_conn import Neo4jService

# Загрузка и проверка переменных окружения
load_dotenv()
REQUIRED_ENV_VARS = ["NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "MEILISEARCH_URL", "BETTERSTACK_TOKEN", "BETTERSTACK_URL"]
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        raise ValueError(f"Missing required environment variable: {var}")

# Конфигурация
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
MEILISEARCH_URL = os.getenv("MEILISEARCH_URL")
MEILISEARCH_API_KEY = os.getenv("MEILISEARCH_API_KEY", "")
BETTERSTACK_TOKEN = os.getenv("BETTERSTACK_TOKEN")
BETTERSTACK_URL = os.getenv("BETTERSTACK_URL")

# Настройка логирования
logger = setup_logging(token=BETTERSTACK_TOKEN, url=BETTERSTACK_URL)

# Инициализация шедулера
scheduler = AsyncIOScheduler()

# Lifespan handler для управления шедулером
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting scheduler")
    scheduler.start()
    yield
    logger.info("Shutting down scheduler")
    scheduler.shutdown()

# Инициализация FastAPI
app = FastAPI(
    lifespan=lifespan,
    title="Article Search Microservice",
    description="API for searching articles using Meilisearch and Neo4j",
    version="1.0.0"
)

# Middleware для логирования HTTP-запросов
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    response = await call_next(request)
    logger.info(f"Response: {response.status_code}")
    return response

# Модель для запроса поиска
class SearchQuery(BaseModel):
    query: str = Field(..., max_length=500)
    """Search query string for article lookup (max 500 characters)."""

# Модель для ответа
class SearchResult(BaseModel):
    id: str
    title: str
    description: str
    """Article data with ID, title, and description."""

# Подключение к Meilisearch
async def get_meili_client() -> MeiliClient:
    client = MeiliClient(MEILISEARCH_URL, MEILISEARCH_API_KEY)
    logger.info("Meilisearch client initialized")
    return client

# Функция реиндексации
async def reindex_articles():
    try:
        logger.info("Starting reindexing...")
        with Neo4jService(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as neo4j_service:
            articles = neo4j_service.get_articles()
            logger.info(f"Retrieved {len(articles)} articles for reindexing")

        async with await get_meili_client() as client:
            try:
                await client.get_index("articles")
            except MeilisearchApiError as e:
                if e.error_code == "index_not_found":
                    await client.create_index("articles", primary_key="id")
                    logger.info("Created Meilisearch index 'articles'")
                else:
                    logger.error(f"Meilisearch error during reindexing: {str(e)}")
                    raise
            index = client.index("articles")
            await index.add_documents(articles)
            logger.info(f"Reindexed {len(articles)} articles in Meilisearch")
    except Exception as e:
        logger.error(f"Reindexing failed: {str(e)}")

# Настройка шедулера
scheduler.add_job(reindex_articles, 'interval', hours=1)

# Эндпоинт для поиска
@app.post(
    "/search",
    response_model=List[SearchResult],
    summary="Search articles",
    description="Search articles by query string using Meilisearch."
)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(MeilisearchApiError),
    before_sleep=lambda retry_state: logger.warning(f"Retrying Meilisearch search: attempt {retry_state.attempt_number}")
)
async def search_articles(query: SearchQuery):
    logger.info(f"Search request for query: {query.query}")
    try:
        async with await get_meili_client() as client:
            index = client.index("articles")
            results = await index.search(query.query, attributes_to_retrieve=["id", "title", "description"])
            logger.info(f"Meilisearch returned {len(results.hits)} hits for query: {query.query}")
            return [SearchResult(**hit) for hit in results.hits]
    except MeilisearchApiError as e:
        logger.error(f"Search failed: Meilisearch error - {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: Meilisearch error - {e.error_code}")
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

# Эндпоинт для проверки состояния
@app.get(
    "/health",
    summary="Health check",
    description="Check the health of Neo4j and Meilisearch connections."
)
async def health_check():
    health_status = {"neo4j": "healthy", "meilisearch": "healthy"}
    logger.info("Health check requested")

    try:
        with Neo4jService(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as neo4j_service:
            neo4j_service.get_articles(limit=1)  # Проверка соединения
    except Exception as e:
        health_status["neo4j"] = f"unhealthy: {str(e)}"
        logger.error(f"Neo4j health check failed: {str(e)}")

    try:
        async with await get_meili_client() as client:
            await client.get_index("articles")  # Проверка соединения
    except Exception as e:
        health_status["meilisearch"] = f"unhealthy: {str(e)}"
        logger.error(f"Meilisearch health check failed: {str(e)}")

    if all(status == "healthy" for status in health_status.values()):
        return {"status": "healthy", "details": health_status}
    else:
        raise HTTPException(status_code=503, detail={"status": "unhealthy", "details": health_status})

# Запуск сервера
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)