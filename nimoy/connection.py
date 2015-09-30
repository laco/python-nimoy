from nimoy.utils import import_class
from nimoy.schemas import DBSchema
import uuid as _uuid


class DatabaseConnection(object):

    def __init__(self, **kwargs):
        backend_cls = kwargs.pop('backend', None)
        schema = kwargs.pop('schema', None)

        if backend_cls is None:
            raise AttributeError("Missing 'backend' parameter.")
        if schema is None:
            raise AttributeError("Missing 'schema' parameter.")
        elif isinstance(schema, dict):
            self.schema = DBSchema.from_dict(schema)
        elif isinstance(schema, str):
            self.schema = DBSchema.from_json(schema)

        self.options = kwargs
        self.backend = import_class(backend_cls)(self)

    def _add_version_info(self, schema_name, _data, **kwargs):
        _data['__sv'] = self.schema.get_fingerprint(schema_name)
        return _data

    def put_item(self, schema_name, _data, overwrite=False):
        return self.backend.put_item(
            schema_name,
            self._add_version_info(
                schema_name,
                self.backend.prepare_data(
                    schema_name,
                    self.schema.validate_data(
                        schema_name, _data))),
            overwrite=overwrite)

    def get_item(self, schema_name, _id, **kwargs):
        return self.backend.get_item(schema_name, _id, **kwargs)

    def delete_item(self, schema_name, _id, **kwargs):
        return self.backend.delete_item(schema_name, _id, **kwargs)

    def query(self, schema_name, _w, limit=10, **kwargs):
        return self.backend.query(schema_name, _w, limit, **kwargs)

    def scan(self, schema_name, _w, limit=10, **kwargs):
        return self.backend.scan(schema_name, _w, limit, **kwargs)

    def query_count(self, schema_name, _w, **kwargs):
        return self.backend.query_count(schema_name, _w, **kwargs)

    def uuid(self):
        return _uuid.uuid4().hex
