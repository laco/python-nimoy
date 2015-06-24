from nimoy.connection import DatabaseConnection


def init_adapter(cn):
    nimoy_config = cn.g_('app_config').get('nimoy_config', {})
    nimoy_schemas = cn.g_('app_config').get('nimoy_schemas', {})
    db = DatabaseConnection(schemas=nimoy_schemas, **nimoy_config)
    cn.s_('nimoy_db', db)


def get_item(cn, schema_name, _id):
    return cn.g_('nimoy_db').get_item(schema_name, _id)


def put_item(cn, schema_name, _data):
    return cn.g_('nimoy_db').put_item(schema_name, _data)


def delete_item(cn, schema_name, _id):
    return cn.g_('nimoy_db').delete_item(schema_name, _id)
