import json
from nimoy.utils import import_class, fingerprint as _fp, ensure_key_order
from collections import OrderedDict


class DocumentSchema(object):

    def __init__(self, fields, indexes, **options):
        self._fields = OrderedDict([(fname, self._parse_field(fops)) for fname, fops in fields.items()])
        self._indexes = [self._parse_index(index) for index in indexes]
        self._options = options

    @classmethod
    def from_json(cls, json_string=None):
        json_document = json.loads(json_string)
        return cls.from_dict(json_document)

    @classmethod
    def from_dict(cls, schema_dict):
        return cls(fields=schema_dict.get('fields', {}),
                   indexes=schema_dict.get('indexes', []),
                   **schema_dict.get('options', {}))

    def to_dict(self):
        return ensure_key_order({
            'fields': {fname: self._field_ops_to_dict(fops) for fname, fops in self._fields.items()},
            'indexes': self._indexes,
            'options': self._options
        })

    def to_json(self):
        return json.dumps(self.to_dict(), sort_keys=True)

    def _parse_field(self, options):
        return {
            'type': import_class(options.get('type', 'TextField'), default_prefix='nimoy.fields'),
            'required': options.get('required', False)
        }

    def _field_ops_to_dict(self, options):
        return {
            'type': options['type'].to_text(),
            'required': options['required']
        }

    def _parse_index(self, index):
        def _is_valid_field(field):
            return field in self._fields
        if all([_is_valid_field(field) for field in index]):
            return index
        else:  # pragma: nocover
            raise Exception("Invalid index {}".format(str(index)))

    @property
    def fingerprint(self):
        return _fp(self.to_json())

    @property
    def primary_key(self):
        return self._indexes[0]

    def validate_data(self, _data):
        return _data


class DBSchema(object):

    def __init__(self, schemas=None, **options):
        schemas = schemas or {}
        self._schemas = {name: self._parse_schema_dict(schema_dict) for name, schema_dict in schemas.items()}
        self._options = options

    @classmethod
    def from_json(cls, json_string=None):
        json_document = json.loads(json_string)
        return cls.from_dict(json_document)

    @classmethod
    def from_dict(cls, schema_dict, **options):
        return cls(schemas=schema_dict, **options)

    def _parse_schema_dict(self, schema_dict):
        return DocumentSchema(fields=schema_dict['fields'], indexes=schema_dict['indexes'], **schema_dict.get('options', {}))

    def to_json(self):
        return json.dumps(self.to_dict(), sort_keys=True)

    def to_dict(self):
        return ensure_key_order({
            'schemas': {sname: schema.to_dict() for sname, schema in self._schemas.items()},
            'options': self._options
        })

    def validate_data(self, schema_name, _data):
        if schema_name in self._schemas:
            return self._schemas[schema_name].validate_data(_data)
        raise ValueError("Invalid data for {}: {}".format(schema_name, _data))

    def get_fingerprint(self, schema_name):
        return self._schemas[schema_name].fingerprint

    def get_primary_key(self, schema_name):
        return self._schemas[schema_name].primary_key
