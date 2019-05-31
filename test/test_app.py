import pytest

from app.app import app


@pytest.fixture
def client():
    client = app.test_client()
    yield client


def test_dispatch(client):
    rv = client.post('/', json={
        'usename': 'flask',
        'password': 'secret'
    })
    assert rv.status_code == 200
    print(rv.get_json())
