import logging
from neo4j import GraphDatabase
from typing import List
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

logger = logging.getLogger('ArticleSearch')

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
        Retrieve articles from Neo4j with pagination.

        Args:
            skip (int): Number of articles to skip (default: 0).
            limit (int): Maximum number of articles to return (default: 100).

        Returns:
            List of dictionaries containing article id, title, and description.
        """
        with self.driver.session() as session:
            result = session.run(
                "MATCH (n:Article) RETURN n.id, n.title, n.description SKIP $skip LIMIT $limit",
                skip=skip,
                limit=limit
            )
            articles = [{"id": record["n.id"], "title": record["n.title"], "description": record["n.description"]} for record in result]
            logger.info(f"Retrieved {len(articles)} articles from Neo4j (skip={skip}, limit={limit})")
            return articles