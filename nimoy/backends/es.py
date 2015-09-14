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
        return '_n_'.join([str(_data[key]) for key in key_names])

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
        return self.scan(schema_name, _w, limit)

    def scan(self, schema_name, _w, limit=10):
        query = elastic_parse_wt(_w, {})
        query["size"] = limit
        result = self._es.search(index=schema_name, doc_type=schema_name, body=query)
        return [hit['_source'] for hit in result["hits"]["hits"]]

    def query_count(self, schema_name, _w):
        query = elastic_parse_wt(_w, {})
        result = self._es.count(index=schema_name, doc_type=schema_name, body=query)
        return result.get('count', 0)


def elastic_parse_wt(wt, fmap):

    def _es_match_all(r, f, v):
        return {"match_all": {}}

    def _get_fn_from_fmap(f):
        return fmap.get(f, lambda v: v)

    def _es_exists_rel(r, f, v):
        return {"exists": {"field": f}}

    def _es_not_exists_rel(r, f, v):
        return {"not": _es_exists_rel(r, f, v)}

    def _es_nin_rel(r, f, v):
        return {"not": {"terms": {f: [_get_fn_from_fmap(f)(v_) for v_ in v]}}}

    def _es_in_rel(r, f, v):
        return {"terms": {f: [_get_fn_from_fmap(f)(v_) for v_ in v]}}

    def _es_contains_rel(r, f, v):
        return _es_in_rel("in", f, [v])

    def _es_ncontains_rel(r, f, v):
        return _es_nin_rel("nin", f, [v])

    def _es_range_rel(r, f, v):
        return {"range": {f: {r: _get_fn_from_fmap(f)(v)}}}

    def _es_missing_rel(r, f, v):
        return {"missing": {"field": f}}

    def _es_eq_rel(r, f, v):
        return {"term": {f: _get_fn_from_fmap(f)(v)}}

    def _es_neq_rel(r, f, v):
        return {"not": {"term": {f: _get_fn_from_fmap(f)(v)}}}

    def _es_startswith(r, f, v):
        return {"regexp": {f: "{}.*".format(v)}}

    def _es_match(r, f, v):
        return _es_eq_rel(r, "{}.ngrams".format(f), v)

    es_simple_relation_map = {"from": _es_range_rel, "to": _es_range_rel,
                              "gte": _es_range_rel, "lte": _es_range_rel,
                              "gt": _es_range_rel, "lt": _es_range_rel,
                              "in": _es_in_rel, "nin": _es_nin_rel,
                              "missing": _es_missing_rel,
                              "contains": _es_contains_rel, "ncontains": _es_ncontains_rel,
                              "eq": _es_eq_rel, "neq": _es_neq_rel,
                              "startswith": _es_startswith,
                              "match": _es_match,
                              "exists": _es_exists_rel, "notexists": _es_not_exists_rel,
                              "match_all": _es_match_all}

    def _filters(wt, fmap):
        if wt[0] in ["and", "or"]:
            ret = {wt[0]: [_filters(x, fmap) for x in wt[1:]]}
        else:
            (relation, field, value) = wt
            ret = es_simple_relation_map[relation](*wt)
        return ret

    return {"filter": _filters(wt, fmap)}
