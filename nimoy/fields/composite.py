class BaseCompositeField(object):

    @classmethod
    def to_text(cls):
        return '.'.join(['nimoy.fields', cls.__name__])


class DictField(BaseCompositeField):
    pass


class ListField(BaseCompositeField):
    pass
