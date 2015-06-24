from .base import BaseBackend
try:
    from boto.dynamodb2.table import Table
except ImportError:
    Table = None


class DynamoDBBackend(BaseBackend):
    def __init__(self, conn):
        if Table is None:
            raise ImportError("Plz. install boto library for DynamoDBBackend.")
        super().__init__(conn)

    def _kwargs_for_id(self, schema_name, _id):
        key_names = self._conn.schema.get_primary_key(schema_name)
        if len(key_names) > 1:
            kwargs = {name: _id[idx] for idx, name in enumerate(key_names)}
        else:
            kwargs = {key_names[0]: _id}
        return kwargs

    def put_item(self, schema_name, _data):
        table = Table(schema_name)
        return table.put_item(data=_data)

    def get_item(self, schema_name, _id):
        table = Table(schema_name)
        return table.get_item(**self._kwargs_for_id(schema_name, _id))

    def delete_item(self, schema_name, _id):
        table = Table(schema_name)
        return table.delete_item(**self._kwargs_for_id(schema_name, _id))

    def query(self, schema_name, _w, limit=10):
        table = Table(schema_name)
        kwargs = parse_w_for_kwargs(_w)
        kwargs['limit'] = limit
        return table.query_2(**kwargs)

    def scan(self, schema_name, _w, limit=10):
        table = Table(schema_name)
        kwargs = parse_w_for_kwargs(_w)
        kwargs['limit'] = limit
        return table.scan(**kwargs)


def parse_w_for_kwargs(_w):
    ret = {}
    if _w[0] == 'and':
        ret.update(parse_w_for_kwargs(_w[1]))
        ret.update(parse_w_for_kwargs(_w[2]))
    elif _w[0] in ['eq', 'beginswith', 'lte', 'gte', 'lt', 'gt']:  # FIXME
        ret['__'.join([_w[1], _w[0]])] = _w[2]
    return ret
