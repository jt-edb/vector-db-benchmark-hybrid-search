import multiprocessing as mp
from typing import List, Tuple

import psycopg
from psycopg.rows import dict_row

from engine.base_client.search import BaseSearcher
from engine.base_client.distances import Distance
from engine.clients.pgvectorall.config import PGVECTOR_COLLECTION_NAME
from engine.clients.pgvectorall.parser import PgvectorAllConditionParser


class PgvectorAllSearcher(BaseSearcher):
    DISTANCE_MAPPING_OPS = {
        Distance.L2: "<->",
        Distance.COSINE: "<=>",
        Distance.DOT: "<#>",
    }

    search_params = {}
    distance = None
    client: psycopg.Connection = None
    parser = PgvectorAllConditionParser()

    @classmethod
    def init_client(cls, host, distance, connection_params: dict, search_params: dict):
        cls.client = psycopg.connect(f"host={host} dbname=postgres", **connection_params)
        cls.search_params = search_params
        cls.distance = distance
        ef_search = search_params.get('hnsw_config', {}).get('ef_search', 64)
        with cls.client.cursor() as cur:
            cur.execute(f"SET hnsw.ef_search = {ef_search};")

    @classmethod
    def get_mp_start_method(cls):
        return "spawn"

    @classmethod
    def search_one(cls, vector, meta_conditions, top) -> List[Tuple[int, float]]:

        vector = "[" + ", ".join(str(x) for x in vector) + "]"
        meta_conditions = cls.parser.parse(meta_conditions)

        if cls.distance == Distance.L2:
            query = f"""
                SELECT id, distance FROM (
            """
        elif cls.distance == Distance.COSINE:
            query = f"""
                SELECT id, 1 - distance FROM (
            """
        elif cls.distance == Distance.DOT:
            query = f"""
                SELECT id, -1 * distance FROM (
            """

        query += f"""
                SELECT
                    id,
                    (embedding {cls.DISTANCE_MAPPING_OPS[cls.distance]} '{vector}') AS distance
                FROM
                    {PGVECTOR_COLLECTION_NAME}

            """
        if meta_conditions is not None:
            query += " WHERE " + meta_conditions
        query += f" ORDER BY distance LIMIT {top}) sq;"

        with cls.client.cursor() as cur:
            cur.execute(query)
            res = cur.fetchall()
            return [(row[0], row[1]) for row in res]

    @classmethod
    def delete_client(cls):
        if cls.client is not None:
            cls.client.close()
        del cls
