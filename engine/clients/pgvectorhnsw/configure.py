import psycopg

from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.pgvectorhnsw.config import PGVECTOR_COLLECTION_NAME


class PgvectorHnswConfigurator(BaseConfigurator):
    DISTANCE_MAPPING_IDX_OPS = {
        Distance.L2: "vector_l2_ops",
        Distance.COSINE: "vector_cosine_ops",
        Distance.DOT: "vector_ip_ops",
    }
    FIELD_MAPPING = {
        "int": "integer",
        "keyword": "varchar",
        "text": "text",
        "float": "real",
        "geo": "point",
        "json": "jsonb",
    }

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        self.client = psycopg.connect(f"host={host} dbname=postgres", **connection_params)

    def clean(self):
        with self.client.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {PGVECTOR_COLLECTION_NAME}")
        self.client.commit()

    def recreate(self, dataset: Dataset, collection_params):
        # pgvector HNSW index supports vectors up to 2,000 dimensions
        if dataset.config.vector_size > 2000:
            raise IncompatibilityError

        fields = [
            f'"{field_name}" {self.FIELD_MAPPING.get(field_type)}'
            for field_name, field_type in dataset.config.schema.items()
        ]

        with self.client.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

            create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {PGVECTOR_COLLECTION_NAME} (
                    id serial PRIMARY KEY,
                    embedding vector({dataset.config.vector_size}) STORAGE MAIN
                """
            if len(fields) > 0:
                create_stmt += ", "
            create_stmt += ", ".join(fields)
            create_stmt += ")"
            cur.execute(create_stmt)

            # HNSW Index creation
            m = collection_params['hnsw_config']['m']
            ef_construction = collection_params['hnsw_config']['ef_construction']
            create_idx_stmt = f"""
                CREATE INDEX ON {PGVECTOR_COLLECTION_NAME} USING hnsw (embedding {self.DISTANCE_MAPPING_IDX_OPS[dataset.config.distance]})
                WITH (m = {m}, ef_construction = {ef_construction})
                """
            cur.execute(create_idx_stmt)

        self.client.commit()
        self.client.close()
