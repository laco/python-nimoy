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
