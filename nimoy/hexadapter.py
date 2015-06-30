from nimoy.connection import DatabaseConnection


def init_adapter(cn):
    nimoy_config = cn.g_('app_config').get('nimoy_config', {})
    nimoy_schemas = cn.g_('app_config').get('nimoy_schemas', {})
    db = DatabaseConnection(schema=nimoy_schemas, **nimoy_config)
    cn.s_('nimoy_db', db)


def get_item(cn, schema_name, _id):
    return cn.g_('nimoy_db').get_item(schema_name, _id)


def put_item(cn, schema_name, _data):
    return cn.g_('nimoy_db').put_item(schema_name, _data)


def delete_item(cn, schema_name, _id):
    return cn.g_('nimoy_db').delete_item(schema_name, _id)


def query(cn, schema_name, _w, limit=10):
    return cn.g_('nimoy_db').query(schema_name, _w, limit)


def query_count(cn, schema_name, _w):
    return cn.g_('nimoy_db').query_count(schema_name, _w)


def scan(cn, schema_name, _w, limit=10):
    return cn.g_('nimoy_db').scan(schema_name, _w, limit)


def uuid(cn):
    return cn.g_('nimoy_db').uuid()
