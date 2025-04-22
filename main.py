import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase
from meilisearch_python_async import Client as MeiliClient
from meilisearch_python_async.errors import MeilisearchApiError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import asyncio
from contextlib import asynccontextmanager
from typing import List

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка и проверка переменных окружения
load_dotenv()
REQUIRED_ENV_VARS = ["NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD", "MEILISEARCH_URL"]
for var in REQUIRED_ENV_VARS:
    if not os.getenv(var):
        logger.error(f"Missing required environment variable: {var}")
        raise ValueError(f"Missing required environment variable: {var}")

# Конфигурация
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
MEILISEARCH_URL = os.getenv("MEILISEARCH_URL")
MEILISEARCH_API_KEY = os.getenv("MEILISEARCH_API_KEY", "")

# Инициализация шедулера
scheduler = AsyncIOScheduler()

# Lifespan handler для управления шедулером
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    logger.info("Scheduler started")
    yield
    scheduler.shutdown()
    logger.info("Scheduler shutdown")

# Инициализация FastAPI
app = FastAPI(
    lifespan=lifespan,
    title="Article Search Microservice",
    description="API for searching articles using Meilisearch and Neo4j",
    version="1.0.0"
)

# Модель для запроса поиска
class SearchQuery(BaseModel):
    query: str
    """Search query string for article lookup."""

# Модель для ответа
class SearchResult(BaseModel):
    id: str
    title: str
    description: str
    """Article data with ID, title, and description."""

# Подключение к Neo4j с контекстным менеджером
class Neo4jService:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    def get_articles(self) -> List[dict]:
        with self.driver.session() as session:
            result = session.run("MATCH (n:Article) RETURN n.id, n.title, n.description")
            return [{"id": record["n.id"], "title": record["n.title"], "description": record["n.description"]} for record in result]

# Подключение к Meilisearch
async def get_meili_client() -> MeiliClient:
    return MeiliClient(MEILISEARCH_URL, MEILISEARCH_API_KEY)

# Функция реиндексации
async def reindex_articles():
    try:
        logger.info("Starting reindexing...")
        with Neo4jService(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD) as neo4j_service:
            articles = neo4j_service.get_articles()

        async with await get_meili_client() as client:
            try:
                await client.get_index("articles")
            except MeilisearchApiError as e:
                if e.error_code == "index_not_found":
                    await client.create_index("articles", primary_key="id")
                    logger.info("Created Meilisearch index 'articles'")
                else:
                    raise
            index = client.index("articles")
            await index.add_documents(articles)
        logger.info("Reindexing completed.")
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
async def search_articles(query: SearchQuery):
    try:
        async with await get_meili_client() as client:
            index = client.index("articles")
            results = await index.search(query.query, attributes_to_retrieve=["id", "title", "description"])
            return [SearchResult(**hit) for hit in results.hits]
    except MeilisearchApiError as e:
        logger.error(f"Search failed: Meilisearch error - {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: Meilisearch error - {e.error_code}")
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

# Запуск сервера
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)