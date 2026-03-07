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
```

Update `requirements.txt` to add `flask-cors`:
```
flask
flask-cors
requests
gunicorn
```

✅ Checkpoint: your repo has `app.py` and `requirements.txt`

---

**Step 4 — Create Render account**

1. Go to **render.com** and click "Get Started"
2. Click **"Sign up with GitHub"** — this connects the two accounts automatically
3. Authorize Render to access your GitHub

✅ Checkpoint: you see the Render dashboard

---

**Step 5 — Deploy the server**

1. Click **"New +"** → **"Web Service"**
2. Click **"Connect"** next to `kalshi-proxy`
3. Fill in the settings:
   - **Name:** `kalshi-proxy` (or anything)
   - **Region:** US East or US West (pick whichever)
   - **Branch:** `main`
   - **Runtime:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn app:app`
   - **Instance Type:** `Free`
4. Click **"Deploy Web Service"**

Render will take 2–3 minutes to build and deploy.

✅ Checkpoint: you see a green "Live" badge and a URL like `https://kalshi-proxy-xxxx.onrender.com`

---

**Step 6 — Test it**

Paste this in your browser, replacing with your actual Render URL:
```
https://kalshi-proxy-xxxx.onrender.com/kalshi/events/kxhighaus-26mar07
```

You should see raw Kalshi JSON with bracket markets.

Also test the health endpoint:
```
https://kalshi-proxy-xxxx.onrender.com/health
