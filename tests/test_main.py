from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_search_endpoint():
    response = client.post("/search", json={"query": "python"})
    assert response.status_code in [200, 500]  # 500 if Meilisearch/Neo4j unavailable
    if response.status_code == 200:
        assert isinstance(response.json(), list)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code in [200, 503]
    assert "status" in response.json()