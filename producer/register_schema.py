import requests
from schema import TRANSACTION_SCHEMA_STR

SCHEMA_REGISTRY_URL = "http://localhost:8081"  # Host view (change to schema-registry:8081 if running inside container)
SUBJECT_NAME = "transactions-raw-value"  # Convention: topic-name-key/value


def register_schema():
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": TRANSACTION_SCHEMA_STR}

    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT_NAME}/versions",
        headers=headers,
        json=payload,
    )

    if response.status_code == 200:
        print(f"Schema registered successfully: {response.json()}")
    else:
        print(f"Error: {response.status_code} - {response.text}")


if __name__ == "__main__":
    register_schema()
