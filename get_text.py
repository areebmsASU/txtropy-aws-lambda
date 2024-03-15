import os
import json
import psycopg2


def lambda_handler(event, context):
    query_string_parameters = event.get("queryStringParameters") or {}
    path_parameters = event.get("pathParameters") or {}

    book = path_parameters.get("bookId")
    if book is None:
        return {"statusCode": 400, "body": "Bad Request"}

    i = query_string_parameters.get("i", 0)
    limit = query_string_parameters.get("limit", os.environ["DEFAULT_LIMIT"])

    query = """SELECT "textropy_line"."rel_id", "textropy_line"."text" 
    FROM "textropy_line" INNER JOIN "textropy_book" ON ("textropy_line"."book_id" = "textropy_book"."id") 
    """

    query += f'WHERE ("textropy_book"."gutenberg_id" = {book} AND "textropy_line"."rel_id" >= {i})'
    query += f'ORDER BY "textropy_line"."rel_id" ASC LIMIT {limit}'

    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )
    cursor = connection.cursor()
    cursor.execute(query)
    query_results = list(cursor.fetchall())
    cursor.close()
    connection.commit()

    results = []
    for rel_id, text in query_results:
        results.append({"id": rel_id, "text": text})

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(results),
    }
