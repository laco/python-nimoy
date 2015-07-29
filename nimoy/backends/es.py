from nimoy.exceptions import ItemNotFound
from .base import BaseBackend

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import NotFoundError
except ImportError:
    Elasticsearch = None
    NotFoundError = None
    # TODO: elasticsearch.exceptions.ConflictError !!!


class ElasticsearchBackend(BaseBackend):
    def __init__(self, conn):
        if Elasticsearch is None:
            raise ImportError("Plz. install elasticsearch library for ElasticsearchBackend.")
        self._es = Elasticsearch(**conn.options.get('elasticsearch', {}))
        super().__init__(conn)

    def _gen_es_id_for_data(self, schema_name, _data):
        key_names = self._conn.schema.get_primary_key(schema_name)
        return '_n_'.join([_data[key] for key in key_names])

    def _gen_es_id_for_id(self, _id):
        if isinstance(_id, str):
            return _id
        elif isinstance(_id, (tuple, list)):
            return '_n_'.join(_id)
        else:
            return _id

    def put_item(self, schema_name, _data, overwrite=False):
        op_type = 'create' if not overwrite else 'index'
        result = self._es.index(index=schema_name, doc_type=schema_name, id=self._gen_es_id_for_data(schema_name, _data), body=_data, op_type=op_type)
        return result.get('_version', 0) > 0

    def get_item(self, schema_name, _id):
        try:
            result = self._es.get(index=schema_name, doc_type=schema_name, id=self._gen_es_id_for_id(_id))
        except NotFoundError:
            raise ItemNotFound("Item not found for id {} in {}.".format(_id, schema_name))
        return result['_source']

    def delete_item(self, schema_name, _id):
        result = self._es.delete(index=schema_name, doc_type=schema_name, id=self._gen_es_id_for_id(_id))
        return result['found'] is True

    def query(self, schema_name, _w, limit=10):
        raise NotImplemented

    def scan(self, schema_name, _w, limit=10):
        raise NotImplemented

    def query_count(self, schema_name, _w):
        raise NotImplemented
