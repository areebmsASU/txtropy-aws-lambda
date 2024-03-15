import os
import json
import psycopg2


def lambda_handler(event, context):
    query_string_parameters = event.get("queryStringParameters") or {}
    path_parameters = event.get("pathParameters") or {}

    book = path_parameters.get("bookId")
    if book is None:
        return {"statusCode": 400, "body": "Bad Request"}

    query = f'SELECT "gutenberg_book"."html_map" FROM "gutenberg_book" WHERE "gutenberg_book"."gutenberg_id" = {book}'

    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()[0]
    cursor.close()
    connection.commit()

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(result),
    }
