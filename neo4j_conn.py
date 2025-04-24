import logging
from neo4j import GraphDatabase
from typing import List
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import json

logger = logging.getLogger('ArticleSearch')

def extract_text_from_json(obj: any) -> str:
    """
    Recursively extract all text values from a JSON object.

    Args:
        obj: JSON object (dict, list, str, or other).

    Returns:
        String containing all text values joined by spaces.
    """
    texts = []
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "text" and isinstance(value, str):
                texts.append(value)
            else:
                texts.append(extract_text_from_json(value))
    elif isinstance(obj, list):
        for item in obj:
            texts.append(extract_text_from_json(item))
    elif isinstance(obj, str):
        texts.append(obj)
    
    return " ".join(text for text in texts if text)

class Neo4jService:
    """
    Service for interacting with Neo4j database with connection pooling.

    Args:
        uri (str): Neo4j database URI.
        user (str): Neo4j username.
        password (str): Neo4j password.
    """
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_pool_size=100)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logger.warning(f"Retrying Neo4j query: attempt {retry_state.attempt_number}")
    )
    def get_articles(self, skip: int = 0, limit: int = 100) -> List[dict]:
        """
        Retrieve articles from Neo4j with pagination, including contentJson.

        Args:
            skip (int): Number of articles to skip (default: 0).
            limit (int): Maximum number of articles to return (default: 100).

        Returns:
            List of dictionaries containing article id, title, description, and content.
        """
        with self.driver.session() as session:
            result = session.run(
                "MATCH (n:Article) RETURN n.id, n.title, n.description, n.contentJson SKIP $skip LIMIT $limit",
                skip=skip,
                limit=limit
            )
            articles = []
            for record in result:
                article = {
                    "id": record["n.id"],
                    "title": record["n.title"],
                    "description": record["n.description"],
                    "content": ""
                }
                # Логируем содержимое contentJson для отладки
                content_json_raw = record["n.contentJson"]
                logger.debug(f"Raw contentJson for article {record['n.id']}: {content_json_raw}")
                
                # Обрабатываем contentJson
                if content_json_raw:
                    try:
                        # Проверяем, является ли contentJson строкой
                        if isinstance(content_json_raw, str):
                            try:
                                # Пытаемся разобрать как JSON
                                content_json = json.loads(content_json_raw)
                            except json.JSONDecodeError:
                                # Если не JSON, используем как строку
                                content_json = content_json_raw
                        else:
                            content_json = content_json_raw

                        # Извлекаем текст рекурсивно
                        content_text = extract_text_from_json(content_json)
                        article["content"] = content_text
                        logger.debug(f"Extracted content for article {record['n.id']}: {content_text}")
                    except Exception as e:
                        logger.warning(f"Failed to process contentJson for article {record['n.id']}: {str(e)}")
                        article["content"] = str(content_json_raw)
                else:
                    logger.debug(f"No contentJson for article {record['n.id']}")
                
                articles.append(article)
            logger.info(f"Retrieved {len(articles)} articles from Neo4j (skip={skip}, limit={limit})")
            return articles