"""
app/main.py  —  Medikart Analytics Flask App
Reads JSON data from GCP Cloud Storage and serves the dashboard.
"""
import os, json, logging
from flask import Flask, jsonify, send_file, abort
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
app = Flask(__name__, static_folder="static")

PROJECT_ID  = "medikart-494016"
BUCKET_NAME = os.environ.get("BUCKET_NAME", "medikart-494016-data")
CACHE       = {}   # simple in-memory cache

def read_json(filename: str):
    """Read a JSON file from Cloud Storage with simple caching."""
    if filename in CACHE:
        return CACHE[filename]
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob   = bucket.blob(filename)
        if not blob.exists():
            return None
        data = json.loads(blob.download_as_text())
        CACHE[filename] = data
        return data
    except Exception as e:
        logging.error(f"Error reading {filename}: {e}")
        return None


def clear_cache():
    CACHE.clear()


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/daybook")
def api_daybook():
    data = read_json("daybook.json")
    if data is None:
        return jsonify({"error": "daybook.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/products")
def api_products():
    data = read_json("products.json")
    if data is None:
        return jsonify({"error": "products.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/stock")
def api_stock():
    data = read_json("stock.json")
    if data is None:
        return jsonify({"error": "stock.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/customers")
def api_customers():
    data = read_json("customers.json")
    if data is None:
        return jsonify({"error": "customers.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/suppliers")
def api_suppliers():
    data = read_json("suppliers.json")
    if data is None:
        return jsonify({"error": "suppliers.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/gst")
def api_gst():
    data = read_json("gst.json")
    if data is None:
        return jsonify({"error": "gst.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/outstanding")
def api_outstanding():
    data = read_json("outstanding.json")
    if data is None:
        return jsonify({"error": "outstanding.json not found in bucket"}), 404
    return jsonify(data)


@app.route("/api/refresh")
def api_refresh():
    """Clear cache so next request re-reads from bucket."""
    clear_cache()
    return jsonify({"status": "cache cleared"})


@app.route("/api/status")
def api_status():
    """Health check — shows which data files are available."""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        files  = [b.name for b in bucket.list_blobs()]
        return jsonify({
            "status":    "ok",
            "bucket":    BUCKET_NAME,
            "files":     files,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── Frontend ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_file("static/index.html")

@app.route("/<path:path>")
def catch_all(path):
    if path.startswith("api/"):
        abort(404)
    return send_file("static/index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
