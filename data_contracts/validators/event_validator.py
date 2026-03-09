import json
from jsonschema import validate, ValidationError

SCHEMA_PATH = "data_contracts/schemas/user_events_schema.json"

with open(SCHEMA_PATH) as f:
    schema = json.load(f)


def validate_event(event):

    try:
        validate(instance=event, schema=schema)
        return True

    except ValidationError as e:
        print("Schema validation failed:", e.message)
        return False
