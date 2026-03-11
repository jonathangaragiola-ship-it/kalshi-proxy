import requests
from flask import Flask, jsonify
from flask_cors import CORS
from kalshi_auth import test_auth, kalshi_get

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

@app.route("/auth-test")
def auth_test():
    try:
        data = kalshi_get("/trade-api/v2/portfolio/balance")
        return jsonify({"status": "ok", "data": data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run()

@app.route("/kalshi/raw")
def raw():
    from flask import request
    url = f"{KALSHI_BASE}/markets?{request.query_string.decode()}"
    r = requests.get(url, headers=HEADERS, timeout=10)
    return (r.content, r.status_code, {"Content-Type": "application/json"})

@app.route('/metar/<station>')
def metar(station):
    from flask import request
    hours = request.args.get('hours', None)
    url = f'https://aviationweather.gov/api/data/metar?ids={station}&format=json&taf=false'
    if hours:
        url += f'&hours={hours}'
    r = requests.get(url, timeout=10)
    return jsonify(r.json())
