import json
from nimoy.schemas import DocumentSchema
from nimoy.fields import TextField, IntegerField
from nimoy.utils import ensure_key_order


def test_document_schema_from_json():
    sample_json = {
        'fields': {
            'name': {'type': 'nimoy.fields.TextField', 'required': False},
            'email': {'type': 'nimoy.fields.TextField', 'required': True}
            },
        'indexes': [('email',)],
    }
    schema = DocumentSchema.from_json(json_string=json.dumps(sample_json))
    assert schema is not None
    assert isinstance(schema, DocumentSchema)
    assert schema._fields['name']['type'] == TextField


def test_document_schema_init():
    schema = DocumentSchema(
        fields={
            'email': {'type': TextField}
        },
        indexes=[['email']])
    assert schema._fields['email']['type'] == TextField
    assert schema._fields['email']['required'] is False
    assert schema._indexes == [['email']]


def test_document_schema_to_dict():
    sample_json2 = json.dumps(ensure_key_order({
        'fields': {'email': {'type': 'nimoy.fields.TextField', 'required': True}},
        'indexes': [['email']],
        'options': {},
    }))
    schema = DocumentSchema.from_json(sample_json2)
    sample_dict = schema.to_dict()
    sample_json = schema.to_json()
    assert sample_dict is not None
    assert sample_dict['fields']['email']['type'] == 'nimoy.fields.TextField'
    assert sample_json == sample_json2


def test_document_schema_hash_exists():
    schemas = [
        DocumentSchema(fields={'email': {'type': TextField}}, indexes=[['email']]),
        DocumentSchema(fields={'email2': {'type': TextField}}, indexes=[['email2']]),
        DocumentSchema(fields={'email': {'type': IntegerField}}, indexes=[['email']]),
    ]
    fingerprints_take_1 = [schema.fingerprint for schema in schemas]
    fingerprints_take_2 = [schema.fingerprint for schema in schemas]
    assert fingerprints_take_1 == fingerprints_take_2 \
        == ['a3e0b912192a511ca8676a54305cda3764c14116', '1a7a2edc1ae1c4730a6383e5ce086aab6ee0d882', 'de6ab7f18942c5914f7df9559bb964b6ba55bae8']
