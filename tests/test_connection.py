import pytest
from nimoy.connection import DatabaseConnection


def test_database_connection_wo_backend():
    with pytest.raises(AttributeError):
        DatabaseConnection()


def test_database_connection_w_inmemory_backend():
    db = DatabaseConnection(
        schema={
            "users": {
                "fields": {
                    'name': {'type': 'TextField', 'required': False},
                    'email': {'type': 'TextField', 'required': True}
                },
                "indexes": [('email',)],
            }}, backend='nimoy.backends.in_memory.InMemoryBackend')
    result = db.put_item('users', _data={
        'email': 'test@example.com', 'name': 'gipsz jakab', 'pw': '123456'})
    sample_user = db.get_item('users', _id='test@example.com')

    assert db is not None
    assert result is not None
    assert sample_user['email'] == 'test@example.com'
    assert sample_user['pw'] == '123456'
