import os
import json
import psycopg2
import psycopg2.extras


ATTRS = [
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

    exclude_clause = (
        f'NOT ( "gutenberg_entropy"."{field}" = 0.0 ) AND' if exclude_zero else ""
    )
    """
    Entropy.objects.exclude(rel_entr=0).filter(
        chunk__book_builder_id__in=list(range(10))
    ).order_by(f"rel_entr_normed").values(
        "chunk__book_builder_id",
        "related_chunk__book__gutenberg_id",
        "related_chunk__book_builder_id",
        "related_chunk__text",
        "rel_entr",
        "rel_entr_normed",
        "shared_count_x_rel_entr",
        "shared_count_x_rel_entr_normed",
        "combined_count_x_rel_entr",
        "combined_count_x_rel_entr_normed",
        "combined_count_x_rel_entr_1",
        "combined_count_x_rel_entr_normed_1",
    ).distinct()[
        :10
    ].query
    """

    return f"""
    SELECT DISTINCT "gutenberg_chunk"."book_builder_id",
                "gutenberg_book"."gutenberg_id",
                T3."book_builder_id",
                T3."text",
                "gutenberg_entropy"."rel_entr",
                "gutenberg_entropy"."rel_entr_normed",
                "gutenberg_entropy"."shared_count_x_rel_entr",
                "gutenberg_entropy"."shared_count_x_rel_entr_normed",
                "gutenberg_entropy"."combined_count_x_rel_entr",
                "gutenberg_entropy"."combined_count_x_rel_entr_normed",
                "gutenberg_entropy"."combined_count_x_rel_entr_1",
                "gutenberg_entropy"."combined_count_x_rel_entr_normed_1"
    FROM   "gutenberg_entropy"
        INNER JOIN "gutenberg_chunk"
                ON ( "gutenberg_entropy"."chunk_id" = "gutenberg_chunk"."id" )
        INNER JOIN "gutenberg_chunk" T3
                ON ( "gutenberg_entropy"."related_chunk_id" = T3."id" )
        INNER JOIN "gutenberg_book"
                ON ( T3."book_id" = "gutenberg_book"."id" )
    WHERE  ( {exclude_clause} 
            "gutenberg_chunk"."book_builder_id" IN ({", ".join(book_builder_ids)}))
    ORDER  BY "gutenberg_entropy"."{field}" ASC
    LIMIT  {str(limit)}
    """


def lambda_handler(event, context):
    query_string_parameters = event.get("queryStringParameters") or {}
    path_parameters = event.get("pathParameters") or {}

    attr = path_parameters.get("attr")
    chunks = query_string_parameters.get("chunks")
    limit = query_string_parameters.get("limit")

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
        }

    connection = psycopg2.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["NAME"],
    )

    results = []
    with connection.cursor() as cursor:
        cursor.execute(get_query(attr, chunk_ids, limit))

        for (
            chunk,
            related_book,
            related_chunk,
            related_text,
            rel_entr,
            rel_entr_normed,
            shared_count_x_rel_entr,
            shared_count_x_rel_entr_normed,
            combined_count_x_rel_entr,
            combined_count_x_rel_entr_normed,
            combined_count_x_rel_entr_1,
            combined_count_x_rel_entr_normed_1,
        ) in cursor.fetchall():
            stats = {k: v for k, v in locals().items() if k in ATTRS}
            results.append(
                {
                    "chunk": chunk,
                    "related": {
                        "book": related_book,
                        "chunk": related_chunk,
                        "text": related_text,
                    },
                    "value": stats.pop(attr),
                    "stats": stats,
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
