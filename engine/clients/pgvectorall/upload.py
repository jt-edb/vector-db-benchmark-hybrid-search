import time
from typing import List, Optional
import psycopg
import json

from qdrant_client import QdrantClient
from qdrant_client.http.models import Batch, CollectionStatus, OptimizersConfigDiff

from engine.base_client.upload import BaseUploader
from engine.clients.pgvectorall.config import PGVECTOR_COLLECTION_NAME


class PgvectorAllUploader(BaseUploader):
    client = None
    host = None
    distance = None
    connection_params = {}
    upload_params = {}

    @classmethod
    def init_client(cls, host, distance, connection_params, upload_params):
        cls.host = host
        cls.distance = distance
        cls.connection_params = connection_params
        cls.upload_params = upload_params

    @staticmethod
    def _update_geo_data(data_object):
        if data_object is None:
            return {}
        keys = data_object.keys()
        for key in keys:
            if isinstance(data_object[key], dict):
                if "lat" in data_object[key] and "lon" in data_object[key]:
                    lat = data_object[key].pop("lat", None)
                    lon = data_object[key].pop("lon", None)
                    data_object[key] = (lat, lon)

        return data_object

    @staticmethod
    def _update_arxiv(data_object):
        filtered_object = dict()

        if data_object is None:
            return {}

        filtered_object['update_date_ts'] = data_object.get('update_date_ts');
        filtered_object['labels'] = json.dumps(data_object.get('labels'));
        filtered_object['submitter'] = data_object.get('submitter');

        return filtered_object

    @classmethod
    def upload_batch(
        cls, ids: List[int], vectors: List[list], metadata: Optional[List[dict]]
    ):
        if cls.client is None:
            cls.client = psycopg.connect(f"host={cls.host} dbname=postgres", **cls.connection_params)

        # Build the COPY statement
        copy_stmt = f"COPY {PGVECTOR_COLLECTION_NAME} FROM STDIN"

        with cls.client.cursor() as cur:
            with cur.copy(copy_stmt) as copy:
                for id_, vector, data_object in zip(ids, vectors, metadata):
                    data_object = PgvectorAllUploader._update_arxiv(data_object)
                    copy.write_row((id_, str(vector)) + tuple(data_object.values()))
        cls.client.commit()

    @classmethod
    def post_upload(cls, _distance):
        return {}

    @classmethod
    def delete_client(cls):
        if cls.client is not None:
            cls.client.close()
            del cls.client
