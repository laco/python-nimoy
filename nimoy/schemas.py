import json
from nimoy.utils import import_class, fingerprint as _fp, ensure_key_order
from collections import OrderedDict


class DocumentSchema(object):

    def __init__(self, fields, indexes, options=None):
        self._fields = OrderedDict([(fname, self._parse_field(fops)) for fname, fops in fields.items()])
        self._indexes = [self._parse_index(index) for index in indexes]
        self._options = options or {}

    @classmethod
    def from_json(cls, json_string=None):
        json_document = json.loads(json_string)
        return cls(fields=json_document.get('fields', {}),
                   indexes=json_document.get('indexes', []),
                   options=json_document.get('options', {}))

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
            'type': import_class(options.get('type', 'nimoy.fields.TextField')),
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
        else:
            raise Exception("Invalid index {}".format(str(index)))

    @property
    def fingerprint(self):
        return _fp(self.to_json())
