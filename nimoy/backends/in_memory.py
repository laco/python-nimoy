from .base import BaseBackend
from nimoy.exceptions import ItemNotFound
from unicodedata import normalize
import operator


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

    def put_item(self, schema_name, _data, overwrite=False, consistent=False):
        if schema_name not in _global_state:
            _global_state[schema_name] = {}
        _global_state[schema_name][self._primary_key(schema_name, _data)] = _data
        return True

    def get_item(self, schema_name, _id, consistent=False):
        try:
            return _global_state[schema_name][_id]
        except (KeyError, AttributeError):
            raise ItemNotFound("Item not found for id {} in {}.".format(_id, schema_name))

    def delete_item(self, schema_name, _id, consistent=False):
        if _id in _global_state[schema_name]:
            del _global_state[schema_name][_id]
        return True

    def _query_or_scan(self, schema_name, _w, limit=10):
        predicate = parse_wt(_w)
        return (v for k, v in _global_state.get(schema_name, {}).items() if predicate(v))

    def query(self, schema_name, _w, limit=10):
        return self._query_or_scan(schema_name, _w, limit)

    def scan(self, schema_name, _w, limit=10):
        return self._query_or_scan(schema_name, _w, limit)

    def query_count(self, schema_name, _w):
        return len(list(self._query_or_scan(schema_name, _w)))


def parse_wt(wt, fmap=None):
    _fmap = fmap or {}

    def _x_f(obj, field):
        if field in _fmap:
            return _fmap[field](obj[field])
        else:
            return obj.get(field)

    def _get_normalized_values(obj, field, value):
        v_normalized = value
        if hasattr(obj[field], 'lower'):
            cobj = _copy(obj)
            cobj[field] = _normalize_and_lower(cobj[field])
            v_normalized = _normalize_and_lower(v_normalized)
            return cobj, v_normalized
        return obj, v_normalized

    def _eq(field, value):
        def _filter(obj):
            return _x_f(obj, field) == value

        def _filter_embed(obj):
            emb_key, emb_field = field.split('.')
            return value in [_x_f(d, emb_field) for d in obj.get(emb_key, [])]
        return _filter_embed if '.' in field else _filter

    def _field_value_predicate(field, value, _operator):
        return lambda obj: _operator(_x_f(obj, field), value)

    def _value_field_predicate(field, value, _operator):
        return lambda obj: _operator(value, _x_f(obj, field))

    def _missing(field, value):
        return lambda obj: operator.is_(_x_f(obj, field), None)

    def _contains(field, value):
        def _filter(obj):
            if obj[field] is not None:
                obj_normalized, v_normalized = _get_normalized_values(obj, field, value)
                if hasattr(v_normalized, 'split'):
                    return all([v_n in _x_f(obj_normalized, field) for v_n in v_normalized.split(" ")])
                return v_normalized in _x_f(obj_normalized, field)
            return False
        return _filter

    def _ncontains(field, value):
        def _filter(obj):
            if obj[field] is not None:
                obj_normalized, v_normalized = _get_normalized_values(obj, field, value)
                if hasattr(v_normalized, 'split'):
                    return not all([v_n in _x_f(obj_normalized, field) for v_n in v_normalized.split(" ")])
                return v_normalized not in _x_f(obj_normalized, field)
            return False
        return _filter

    def _in(field, value):
        return lambda obj: operator.contains(value, _x_f(obj, field))

    def _nin(field, value):
        return lambda obj: operator.not_(operator.contains(value, _x_f(obj, field)))

    def _and(wts):
        _filters = [parse_wt(_wt, fmap=fmap) for _wt in wts]

        def _filter(obj):
            return all([_f(obj) for _f in _filters])
        return _filter

    def _or(wts):
        _filters = [parse_wt(_wt, fmap=fmap) for _wt in wts]

        def _filter(obj):
            return any([_f(obj) for _f in _filters])
        return _filter

    def _startswith(field, value):
        return lambda obj: _x_f(obj, field).startswith(value) if hasattr(_x_f(obj, field), 'startswith') else False

    if wt[0] == 'eq':
        return _eq(wt[1], wt[2])
    elif wt[0] == 'neq':
        return _field_value_predicate(wt[1], wt[2], _operator=operator.ne)
    elif wt[0] == 'gt':
        return _field_value_predicate(wt[1], wt[2], _operator=operator.gt)
    elif wt[0] == 'gte':
        return _field_value_predicate(wt[1], wt[2], _operator=operator.ge)
    elif wt[0] == 'lt':
        return _field_value_predicate(wt[1], wt[2], _operator=operator.lt)
    elif wt[0] == 'lte':
        return _field_value_predicate(wt[1], wt[2], _operator=operator.le)
    elif wt[0] == 'contains':
        return _contains(wt[1], wt[2])
    elif wt[0] == 'ncontains':
        return _ncontains(wt[1], wt[2])
    elif wt[0] == 'in':
        return _in(wt[1], wt[2])
    elif wt[0] == 'nin':
        return _nin(wt[1], wt[2])
    elif wt[0] == 'and':
        return _and(wt[1:])
    elif wt[0] == 'or':
        return _or(wt[1:])
    elif wt[0] == 'startswith':
        return _startswith(wt[1], wt[2])
    elif wt[0] == 'missing':
        return _missing(wt[1], wt[2])


def _copy(d):
    def _copy_list(l):
        r = []
        for i in l:
            if isinstance(i, dict):
                r.append(_copy_dict(i))
            elif isinstance(i, list):
                r.append(_copy_list(i))
            else:
                r.append(i)
        return r

    def _copy_dict(d):
        r = {}
        for i, j in d.items():
            if isinstance(j, dict):
                r[i] = _copy_dict(j)
            elif isinstance(j, list):
                r[i] = _copy_list(j)
            else:
                r[i] = j
        return r

    """ Create a new dict from d """
    return _copy_dict(d)


def _normalize_and_lower(value):
    return normalize('NFKD', value.lower())\
        .encode('ascii', 'ignore')\
        .decode('utf-8')
