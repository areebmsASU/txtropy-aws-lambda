from os import environ
from json import loads

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
    response = get_related(
        event={
            "pathParameters": {"attr": "entr_gained"},
            "queryStringParameters": {"chunks": "29783,29784", "limit": "100"},
        },
        context={},
    )

    body = loads(response["body"])
    assert 0 < len(body) <= 100
    assert response["statusCode"] == 200
    assert type(body) is list
