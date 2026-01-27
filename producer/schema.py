TRANSACTION_SCHEMA_STR = """
{
    "type": "record",
    "name": "Transaction",
    "namespace": "com.omarfg.fraud",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "merchant", "type": "string"},
        {"name": "category", "type": ["string", "null"], "default": null},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "country", "type": "string"},
        {"name": "device_type", "type": "string"},
        {"name": "is_fraud", "type": "boolean", "default": false}
    ]
}
"""
