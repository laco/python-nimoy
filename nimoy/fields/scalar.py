class BaseScalarField(object):

    @classmethod
    def to_text(cls):
        return '.'.join(['nimoy.fields', cls.__name__])


class IntegerField(BaseScalarField):
    pass


class TextField(BaseScalarField):
    pass


class DatetimeField(BaseScalarField):
    pass


class IDField(BaseScalarField):
    pass
