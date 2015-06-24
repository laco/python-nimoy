class BaseBackend(object):
    def __init__(self, conn):
        self._conn = conn

    def prepare_data(self, schema_name, _data):
        return _data

    def put_item(self, schema_name, _data):
        raise NotImplemented()

    def get_item(self, schema_name, _id):
        raise NotImplemented()

    def delete_item(self, schema_name, _id):
        raise NotImplemented()
