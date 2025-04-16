from flask import Flask, request, jsonify
from google.cloud import bigquery, firestore
from google.api_core.exceptions import NotFound
from datetime import datetime
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize Firestore client (uses GOOGLE_CLOUD_PROJECT from env or metadata)
bq_client = bigquery.Client()
db = firestore.Client()

BQ_DATASET = "the_escape_bookings"
ORDERS_TABLE = f"{BQ_DATASET}.orders"
ITEMS_TABLE = f"{BQ_DATASET}.order_items"

@app.before_first_request
def setup_bigquery_tables():
    ensure_dataset(BQ_DATASET)
    ensure_table(ORDERS_TABLE, order_schema())
    ensure_table(ITEMS_TABLE, item_schema())

@app.route("/log", methods=["POST"])
def log_booking():
    data = request.get_json(force=True)

    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    try:
        order_id = str(data.get("id"))
        order_data = {k: v for k, v in data.items() if k != "items"}
        order_data["id"] = order_id

        # 🔹 BigQuery: Insert order
        insert_row_bq(ORDERS_TABLE, order_data)

        # 🔹 Firestore: Store order
        db.collection("orders").document(order_id).set(order_data)

        # 🔹 Insert each item
        for item in data.get("items", []):
            item["order_id"] = order_id

            # BigQuery: insert item
            insert_row_bq(ITEMS_TABLE, item)

            # Firestore: insert item
            db.collection("order_items").add(item)

        return jsonify({"status": "success"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def insert_row_bq(table_id, row_data):
    errors = bq_client.insert_rows_json(table_id, [row_data])
    if errors:
        raise RuntimeError(f"BigQuery insert error: {errors}")

def ensure_dataset(dataset_id):
    try:
        bq_client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(f"{bq_client.project}.{dataset_id}")
        dataset.location = "europe-west6"
        bq_client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")

def ensure_table(table_id, schema):
    try:
        bq_client.get_table(table_id)
    except NotFound:
        table = bigquery.Table(f"{bq_client.project}.{table_id}", schema=schema)
        bq_client.create_table(table)
        print(f"Created table: {table_id}")


def order_schema():
    return [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("pred_order_number", "STRING"),
        bigquery.SchemaField("transaction_number", "STRING"),
        bigquery.SchemaField("total", "INTEGER"),
        bigquery.SchemaField("customer", "RECORD", fields=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("firstname", "STRING"),
            bigquery.SchemaField("lastname", "STRING"),
            bigquery.SchemaField("companyname", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("email_address", "STRING"),
            bigquery.SchemaField("phone1", "STRING"),
            bigquery.SchemaField("phone2", "STRING"),
            bigquery.SchemaField("newsletter_seller", "BOOLEAN"),
        ]),
    ]

def item_schema():
    return [
        bigquery.SchemaField("i_orderitem", "INTEGER"),
        bigquery.SchemaField("i_sku", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("price", "INTEGER"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("slot_start", "TIMESTAMP"),
        bigquery.SchemaField("slot_end", "TIMESTAMP"),
        bigquery.SchemaField("order_id", "STRING"),
    ]


# Optional: health check route for Cloud Run
@app.route("/")
def index():
    return jsonify({"status": "OK"}), 200

# Run locally if needed
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)