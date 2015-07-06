from .base import BaseBackend
from nimoy.exceptions import ItemNotFound

_global_state = {}


class InMemoryBackend(BaseBackend):
    def _primary_key(self, schema_name, _data):
        key_names = self._conn.schema.get_primary_key(schema_name)
        ret = []
        for key in key_names:
            ret.append(_data.get(key))
        if len(ret) == 1:
            return ret[0]
        else:
            return tuple(ret)

    def put_item(self, schema_name, _data, overwrite=False):
        if schema_name not in _global_state:
            _global_state[schema_name] = {}
        _global_state[schema_name][self._primary_key(schema_name, _data)] = _data
        return True

    def get_item(self, schema_name, _id):
        try:
            return _global_state[schema_name].get(_id)
        except KeyError:
            raise ItemNotFound("Item not found for id {} in {}.".format(_id, schema_name))

    def delete_item(self, schema_name, _id):
        if _id in _global_state[schema_name]:
            del _global_state[schema_name][_id]
        return True
