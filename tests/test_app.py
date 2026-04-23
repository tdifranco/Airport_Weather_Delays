import pytest
from app.app import app
"""Basic Flask route tests for the dashboard application.

These tests make sure the main page and common filter views render without
server errors. They are intentionally lightweight and act as smoke tests.
"""
@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

def test_dashboard_loads(client):
    """Main dashboard route should return HTTP 200."""
    response = client.get("/")
    assert response.status_code == 200

def test_airport_filter(client):
    """Dashboard with an airport filter should return HTTP 200."""
    response = client.get("/?airport=ATL")
    assert response.status_code == 200

def test_weather_filter(client):
    """Dashboard with a weather filter should return HTTP 200."""
    response = client.get("/?weather=rain")
    assert response.status_code == 200