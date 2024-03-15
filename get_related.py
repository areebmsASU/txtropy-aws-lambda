import os
import json
import psycopg2


ALLOWED_ATTR = [
    "shared_vocab_count",
    "combined_vocab_count",
    "rel_entr",
    "rel_entr_normed",
    "shared_count_x_rel_entr",
    "shared_count_x_rel_entr_normed",
    "combined_count_x_rel_entr",
    "combined_count_x_rel_entr_normed",
    "combined_count_x_rel_entr_1",
    "combined_count_x_rel_entr_normed_1",
]


def get_query(field, book_builder_ids, limit, exclude_zero=True):

    # Entropy.objects.order_by(f"rel_entr_normed").exclude(rel_entr=0).values(
    #    "related_chunk__book__gutenberg_id",
    #    "related_chunk__book_builder_id",
    #    "related_chunk__text",
    #    "rel_entr_normed",
    # ).query # Missing filter(id__in=)

    exclude_clause = (
        f' AND NOT ("gutenberg_entropy"."{field}" = 0.0))' if exclude_zero else ""
    )

    return f"""
    SELECT "gutenberg_book"."gutenberg_id", "gutenberg_chunk"."book_builder_id", "gutenberg_chunk"."text", "gutenberg_entropy"."{field}" 
    FROM "gutenberg_entropy" 
    INNER JOIN "gutenberg_chunk" ON ("gutenberg_entropy"."related_chunk_id" = "gutenberg_chunk"."id")
    INNER JOIN "gutenberg_book" ON ("gutenberg_chunk"."book_id" = "gutenberg_book"."id") 
    WHERE ("gutenberg_chunk"."book_builder_id" IN ({", ".join(book_builder_ids)}){exclude_clause}
    ORDER BY "gutenberg_entropy"."{field}" ASC
    LIMIT {str(limit)}
    """


def lambda_handler(event, context):
    query_string_parameters = event.get("queryStringParameters") or {}
    path_parameters = event.get("pathParameters") or {}

    attr = path_parameters.get("attr")
    chunks = query_string_parameters.get("chunks")
    limit = query_string_parameters.get("limit")

    if not limit or not chunks or attr not in ALLOWED_ATTR:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            },
        }

    try:
        limit = int(limit)
        chunk_ids = list(map(str, list(map(int, chunks.split(",")))))
    except:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            },
        }

    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )
    with connection.cursor() as cursor:
        cursor.execute(get_query(attr, chunk_ids, limit))
        query_results = list(cursor.fetchall())

    results = []
    for book_id, chunk_id, text, value in query_results:
        results.append(
            {"book": book_id, "chunk": chunk_id, "text": text, "value": value}
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
