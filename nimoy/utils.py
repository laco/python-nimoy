import importlib
import hashlib
from collections import OrderedDict


def import_class(cls_path, default_prefix='nimoy'):
    if isinstance(cls_path, str):
        if '.' not in cls_path:
            cls_path = '.'.join([default_prefix, cls_path])
        dotposition = cls_path.rfind('.')
        module_name = cls_path[:dotposition]
        class_name = cls_path[dotposition + 1:]
        mod = importlib.import_module(module_name)
        return getattr(mod, class_name)
    else:
        return cls_path


def fingerprint(_str):
    sha = hashlib.sha1()
    sha.update(_str.encode('utf-8'))
    return sha.hexdigest()


def ensure_key_order(dictionary):
    ret = OrderedDict()
    for key in sorted(dictionary):
        if isinstance(dictionary[key], dict):
            ret[key] = ensure_key_order(dictionary[key])
        else:
            ret[key] = dictionary[key]
    return ret
