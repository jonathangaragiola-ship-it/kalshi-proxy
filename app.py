import requests
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
HEADERS = {"Content-Type": "application/json"}

@app.route("/kalshi/events/<event_ticker>")
def get_event(event_ticker):
    url = f"{KALSHI_BASE}/markets?event_ticker={event_ticker}&limit=50"
    r = requests.get(url, headers=HEADERS, timeout=10)
    return (r.content, r.status_code, {"Content-Type": "application/json"})

@app.route("/kalshi/series/<series_ticker>")
def get_series(series_ticker):
    url = f"{KALSHI_BASE}/markets?series_ticker={series_ticker}&status=open&limit=50"
    r = requests.get(url, headers=HEADERS, timeout=10)
    return (r.content, r.status_code, {"Content-Type": "application/json"})

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run()

@app.route("/kalshi/raw")
def raw():
    from flask import request
    url = f"{KALSHI_BASE}/markets?{request.query_string.decode()}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    return (r.content, r.status_code, {"Content-Type": "application/json"})
