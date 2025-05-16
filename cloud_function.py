import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery, firestore
from google.cloud.exceptions import NotFound
from google.events.cloud import firestore as firestoredata


# Initialize BigQuery client
bq_client = bigquery.Client()
db = firestore.Client()

# Constants
DATASET = "the_escape_bookings"  # Replace with actual dataset
TABLE_ORDERS = "orders"
TABLE_ITEMS = "order_items"
TABLE_CUSTOMERS = "customers"
TABLE_ORDER_USER = "order_user"
LOCATION = "europe-west6" 

# Inline schema definitions
schemas = {
    TABLE_ORDERS: [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("transaction_number", "STRING"),
        bigquery.SchemaField("total", "FLOAT"),
    ],
    TABLE_ITEMS: [
        bigquery.SchemaField("i_orderitem", "INTEGER"),
        bigquery.SchemaField("i_sku", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("slot_start", "TIMESTAMP"),
        bigquery.SchemaField("slot_end", "TIMESTAMP"),
        bigquery.SchemaField("order_id", "INTEGER"),
    ],
    TABLE_CUSTOMERS: [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("firstname", "STRING"),
        bigquery.SchemaField("lastname", "STRING"),
        bigquery.SchemaField("email_address", "STRING"),
        bigquery.SchemaField("phone1", "STRING"),
        bigquery.SchemaField("phone2", "STRING"),
        bigquery.SchemaField("newsletter", "BOOLEAN"),
    ],
    TABLE_ORDER_USER: [
        bigquery.SchemaField("order_id", "INTEGER"),
        bigquery.SchemaField("customer_id", "STRING"),
    ]
}


def ensure_dataset(dataset_id):
    try:
        bq_client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(f"{bq_client.project}.{dataset_id}")
        dataset.location = LOCATION  # Adjust to your region
        bq_client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")


def ensure_table_exists(table_name):
    table_ref = bq_client.dataset(DATASET).table(table_name)
    try:
        bq_client.get_table(table_ref)
    except NotFound:
        schema = schemas[table_name]
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)


def get_order(data):
    return {
        "id": int(data["id"]),
        "transaction_number": data["transaction_number"],
        "total": float(data["total"])
    }


def get_customer(data):
    customer_fields = data["customer"]
    return {
        "id": customer_fields["id"],
        "firstname": customer_fields["firstname"],
        "lastname": customer_fields["lastname"],
        "email_address": customer_fields["email_address"],
        "phone1": customer_fields["phone1"],
        "phone2": customer_fields.get("phone2", None),
        "newsletter": customer_fields.get("newsletter", False),
    }


def get_items(data, order_id):
    items = []
    for item in data["items"]:
        items.append({
            "i_orderitem": item["i_orderitem"],
            "i_sku": item["i_sku"],
            "name": item["name"],
            "event_name": item["event_name"],
            "quantity": item["quantity"],
            "price": item["price"],
            "slot_start": item["slot_start"],
            "slot_end": item["slot_end"],
            "order_id": order_id
        })
    return items


def build_merge_customer_query(customer):
    
    return f"""
        MERGE `{bq_client.project}.{DATASET}.{TABLE_CUSTOMERS}` T
        USING (SELECT '{customer['id']}' AS id,
                    '{customer['firstname']}' AS firstname,
                    '{customer['lastname']}' AS lastname,
                    '{customer['email_address']}' AS email_address,
                    '{customer['phone1']}' AS phone1,
                    '{customer['phone2']}' AS phone2,
                    {str(customer['newsletter']).upper()} AS newsletter) S
        ON T.id = S.id
        WHEN MATCHED THEN UPDATE SET
            firstname = S.firstname,
            lastname = S.lastname,
            email_address = S.email_address,
            phone1 = S.phone1,
            phone2 = S.phone2,
            newsletter = S.newsletter
        WHEN NOT MATCHED THEN
            INSERT (id, firstname, lastname, email_address, phone1, phone2, newsletter)
            VALUES(S.id, S.firstname, S.lastname, S.email_address, S.phone1, S.phone2, S.newsletter)
        """


@functions_framework.cloud_event
def firestore_to_bigquery(cloud_event: CloudEvent):
    firestore_payload = firestoredata.DocumentEventData()
    firestore_payload._pb.ParseFromString(cloud_event.data)

    # Parse Firestore document path
    path_parts = firestore_payload.value.name.split("/")
    separator_idx = path_parts.index("documents")
    collection_path = path_parts[separator_idx + 1]
    document_path = "/".join(path_parts[(separator_idx + 2):])

    affected_doc = db.collection(collection_path).document(document_path)
    snapshot = affected_doc.get()
    data = snapshot.to_dict()

    order = get_order(data)
    customer = get_customer(data)
    items = get_items(data, order["id"])

    # Ensure dataset and tables exist
    ensure_dataset(DATASET)
    for table in [TABLE_ORDERS, TABLE_ITEMS, TABLE_CUSTOMERS, TABLE_ORDER_USER]:
        ensure_table_exists(table)

    # Insert into orders and items
    bq_client.insert_rows_json(f"{DATASET}.{TABLE_ORDERS}", [order])
    bq_client.insert_rows_json(f"{DATASET}.{TABLE_ITEMS}", items)

    # Merge customer data
    merge_query = build_merge_customer_query(customer)
    bq_client.query(merge_query).result()

    # Insert into order_user
    bq_client.insert_rows_json(f"{DATASET}.{TABLE_ORDER_USER}", [{
        "order_id": order["id"],
        "customer_id": customer["id"]
    }])