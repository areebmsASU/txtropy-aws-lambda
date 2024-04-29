from os import environ
from json import loads

import psycopg2
import psycopg2.extras

from get_books import lambda_handler as get_books
from get_css import lambda_handler as get_css
from get_html_map import lambda_handler as get_html_map
from get_related import lambda_handler as get_related


def test_get_books():
    environ["NAME"] = "bookbuilder"

    response = get_books({}, {})
    body = loads(response["body"])
    assert response["statusCode"] == 200
    assert "id" in body[0]
    assert "title" in body[0]
    assert "author" in body[0]


def test_get_html_map():
    environ["NAME"] = "bookbuilder"
    response = get_html_map(event={"pathParameters": {"bookId": 3300}}, context={})

    body = loads(response["body"])
    assert response["statusCode"] == 200
    assert "tag" in body
    assert "contents" in body


def test_get_css():
    environ["NAME"] = "bookbuilder"
    response = get_css(event={"pathParameters": {"bookId": 3300}}, context={})

    body = response["body"]
    assert response["statusCode"] == 200
    assert type(body) is str
    assert len(body.splitlines()) > 1


def test_get_related():
    environ["NAME"] = "relentropygetter"
    connection = psycopg2.connect(
        user=environ["USER"],
        password=environ["PASSWORD"],
        host=environ["HOST"],
        database=environ["NAME"],
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT "gutenberg_chunk"."book_builder_id"
            FROM "gutenberg_entropy"
            INNER JOIN "gutenberg_chunk"
            ON ("gutenberg_entropy"."chunk_id" = "gutenberg_chunk"."id")
            LIMIT 10"""
        )

        chunk_ids = ",".join([str(chunk_id[0]) for chunk_id in cursor.fetchall()])

    response = get_related(
        event={
            "queryStringParameters": {"chunks": chunk_ids},
        },
        context={},
    )

    body = loads(response["body"])
    assert 0 < len(body) <= 5
    assert response["statusCode"] == 200
    assert type(body) is list
