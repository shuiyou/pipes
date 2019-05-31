import pytest

from app import app


@pytest.fixture
def client():
    client = app.test_client()
    yield client


def test_dispatch(client):
    rp = client.get('/')
    assert rp.status_code == 200
    print(str(rp.data))
