from nimoy.connection import DatabaseConnection


def init_adapter(cn):
    nimoy_config = cn.g_('app_config').get('nimoy_config', {})
    nimoy_schemas = cn.g_('app_config').get('nimoy_schemas', {})
    db = DatabaseConnection(schema=nimoy_schemas, **nimoy_config)
    cn.s_('nimoy_db', db)


def get_item(cn, schema_name, _id, **kw):
    return cn.g_('nimoy_db').get_item(schema_name, _id, **kw)


def put_item(cn, schema_name, _data, **kw):
    return cn.g_('nimoy_db').put_item(schema_name, _data, **kw)


def delete_item(cn, schema_name, _id, **kw):
    return cn.g_('nimoy_db').delete_item(schema_name, _id, **kw)


def query(cn, schema_name, _w, limit=10, **kw):
    return cn.g_('nimoy_db').query(schema_name, _w, limit, **kw)


def query_count(cn, schema_name, _w, **kw):
    return cn.g_('nimoy_db').query_count(schema_name, _w, **kw)


def scan(cn, schema_name, _w, limit=10, **kw):
    return cn.g_('nimoy_db').scan(schema_name, _w, limit, **kw)


def uuid(cn):
    return cn.g_('nimoy_db').uuid()
