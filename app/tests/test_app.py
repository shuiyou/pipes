#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from app import app
from mapping import mysql_reader


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


def test_sql_to_df():
    df = mysql_reader.sql_to_df("channel")
    print(df)
