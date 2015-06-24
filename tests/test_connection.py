import pytest
from nimoy.connection import DatabaseConnection


def test_database_connection_wo_backend():
    with pytest.raises(AttributeError):
        DatabaseConnection()


def test_database_connection_w_inmemory_backend():
    db = DatabaseConnection(
        schema={
            "schemas": {
                "users": {
                    "fields": {
                        'name': {'type': 'nimoy.fields.TextField', 'required': False},
                        'email': {'type': 'nimoy.fields.TextField', 'required': True}
                    },
                    "indexes": [('email',)],
                }}}, backend='nimoy.backends.in_memory.InMemoryBackend')
    result = db.put_item('users', _data={
        'email': 'test@example.com', 'name': 'gipsz jakab', 'pw': '123456'})
    sample_user = db.get_item('users', _id='test@example.com')

    assert db is not None
    assert result is not None
    assert sample_user is not None
