from flask import Flask, request, jsonify
from google.cloud import firestore
from datetime import datetime
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize Firestore client (uses GOOGLE_CLOUD_PROJECT from env or metadata)
db = firestore.Client()

@app.route("/log", methods=["POST"])
def log_booking():

    # Get JSON payload
    data = request.get_json(force=True)

    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    try:
        # Store order (excluding 'items')
        order_id = str(data.get("id"))
        order_data = {k: v for k, v in data.items() if k != "items"}
        db.collection("orders").document(order_id).set(order_data)

        # Store items in a flat order_items collection
        items = data.get("items", [])
        for item in items:
            item["order_id"] = order_id
            db.collection("order_items").add(item)

        return jsonify({"status": "success"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Optional: health check route for Cloud Run
@app.route("/")
def index():
    return jsonify({"status": "OK"}), 200

# Run locally if needed
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)