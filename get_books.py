import os
import json
import psycopg2


def lambda_handler(event, context):
    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )
    cursor = connection.cursor()
    cursor.execute(
        'SELECT "gutenberg_book"."gutenberg_id", "gutenberg_book"."title", "gutenberg_book"."author" FROM "gutenberg_book"'
    )
    query_results = list(cursor.fetchall())
    cursor.close()
    connection.commit()

    results = [
        {"id": gutenberg_id, "title": title, "author": author}
        for gutenberg_id, title, author in query_results
    ]

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(results),
    }
