import os
import json
import psycopg2
import psycopg2.extras


ATTRS = ["shared_vocab_ratio", "shared_vocab_count", "entr_gained", "entr_lost"]


def get_query(field, book_builder_ids, limit, exclude_zero):
    """
    Entropy.objects.filter(chunk__book_builder_id__in=list(range(10)))
    .exclude(entr_lost=0)
    .order_by(f"entr_lost")
    .values(
        "chunk__book_builder_id",
        "related_chunk__book__gutenberg_id",
        "related_chunk__book_builder_id",
        "related_chunk__text",
        "shared_vocab_ratio",
        "shared_vocab_count",
        "shared_vocab_counts",
        "entr_gained",
        "entr_lost",
    )
    .distinct()[:10]
    .query
    """
    if field in ["shared_vocab_ratio", "shared_vocab_count"]:
        order = "DESC"
    else:
        order = "ASC"

    exclude_clause = (
        f'AND NOT ( "gutenberg_entropy"."{field}" = 0.0 )' if exclude_zero else ""
    )
    return f"""
    SELECT DISTINCT "gutenberg_chunk"."book_builder_id",
            "gutenberg_book"."gutenberg_id",
            T3."book_builder_id",
            T3."text",
            "gutenberg_entropy"."shared_vocab_ratio",
            "gutenberg_entropy"."shared_vocab_count",
            "gutenberg_entropy"."shared_vocab_counts",
            "gutenberg_entropy"."entr_gained",
            "gutenberg_entropy"."entr_lost"
    FROM   "gutenberg_entropy"
        INNER JOIN "gutenberg_chunk"
                ON ( "gutenberg_entropy"."chunk_id" = "gutenberg_chunk"."id" )
        INNER JOIN "gutenberg_chunk" T3
                ON ( "gutenberg_entropy"."related_chunk_id" = T3."id" )
        INNER JOIN "gutenberg_book"
                ON ( T3."book_id" = "gutenberg_book"."id" )
    WHERE  ( "gutenberg_chunk"."book_builder_id" IN ({", ".join(book_builder_ids)})
             {exclude_clause} )
    ORDER  BY "gutenberg_entropy"."{field}" {order}
    LIMIT  {limit}
    """


def lambda_handler(event, context):
    query_string_parameters = event.get("queryStringParameters") or {}
    path_parameters = event.get("pathParameters") or {}

    attr = path_parameters.get("attr")
    chunks = query_string_parameters.get("chunks")
    limit = query_string_parameters.get("limit")
    exclude_zero = not int(query_string_parameters.get("zero", 0))

    if not limit or not chunks or attr not in ATTRS:
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
            "body": "Invalid format.",
        }

    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )

    results = []
    with connection.cursor() as cursor:
        cursor.execute(get_query(attr, chunk_ids, limit, exclude_zero=exclude_zero))

        for (
            chunk,
            related_book,
            related_chunk,
            related_text,
            shared_vocab_ratio,
            shared_vocab_count,
            shared_vocab_counts,
            entr_gained,
            entr_lost,
        ) in cursor.fetchall():

            results.append(
                {
                    "chunk": chunk,
                    "related": {
                        "book": related_book,
                        "chunk": related_chunk,
                        "text": related_text,
                    },
                    "value": locals()[attr],
                    "shared": {
                        "ratio": shared_vocab_ratio,
                        "count": shared_vocab_count,
                        "counts": shared_vocab_counts,
                    },
                    "entr": {"gained": entr_gained, "lost": entr_lost},
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
