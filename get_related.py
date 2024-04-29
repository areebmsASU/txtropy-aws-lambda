import os
import json

import psycopg2
import psycopg2.extras


def lambda_handler(event, context):
    query_string_parameters = event.get("queryStringParameters") or {}
    path_parameters = event.get("pathParameters") or {}

    chunks = query_string_parameters.get("chunks")

    if not chunks:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            },
        }

    try:
        chunk_ids = list(map(str, list(map(int, chunks.split(",")))))
    except:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            },
            "body": "Invalid format.",
        }

    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )

    query = f"""
        SELECT "gutenberg_book"."gutenberg_id",
        "gutenberg_book"."title",
        "gutenberg_book"."author",
        T3."book_builder_id",
        T3."text",
        "gutenberg_entropy"."shared_vocab_counts",
        "gutenberg_entropy"."entr_gained",
        "gutenberg_entropy"."entr_lost",
        "gutenberg_entropy"."jensen_shannon"
        FROM   "gutenberg_entropy"
        INNER JOIN "gutenberg_chunk"
                ON ( "gutenberg_entropy"."chunk_id" = "gutenberg_chunk"."id" )
        INNER JOIN "gutenberg_chunk" T3
                ON ( "gutenberg_entropy"."related_chunk_id" = T3."id" )
        INNER JOIN "gutenberg_book"
                ON ( T3."book_id" = "gutenberg_book"."id" )
        WHERE  "gutenberg_chunk"."book_builder_id" IN ( {",".join(map(str,chunk_ids))} )
        ORDER  BY "gutenberg_entropy"."jensen_shannon" ASC
        LIMIT  5
    """

    results = []
    chunk_ids = []
    with connection.cursor() as cursor:
        cursor.execute(query)

        for (
            related_chunk__book__gutenberg_id,
            related_chunk__book__title,
            related_chunk__book__author,
            related_chunk__book_builder_id,
            related_chunk__text,
            shared_vocab_counts,
            entr_gained,
            entr_lost,
            jensen_shannon,
        ) in cursor.fetchall():

            if related_chunk__book_builder_id not in chunk_ids:
                chunk_ids.append(related_chunk__book_builder_id)
                results.append(
                    {
                        "id": related_chunk__book_builder_id,
                        "book": {
                            "id": related_chunk__book__gutenberg_id,
                            "author": related_chunk__book__author,
                            "title": related_chunk__book__title,
                        },
                        "text": related_chunk__text,
                        "entropy": {
                            "jensen_shannon": round(jensen_shannon, 4),
                            "gained": entr_gained,
                            "lost": entr_lost,
                        },
                        "shared_vocab": [
                            {"stem": stem, "count": count}
                            for stem, count in sorted(
                                shared_vocab_counts.items(),
                                key=lambda x: x[1],
                                reverse=True,
                            )
                        ],
                    }
                )

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(results),
    }
