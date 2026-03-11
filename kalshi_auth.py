"""
Kalshi authenticated API client.
Uses RSA-PSS signing as required by Kalshi's API.
Credentials loaded from environment variables — never hardcoded.
"""

import os
import time
import base64
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend


KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def load_private_key():
    pem = os.environ["KALSHI_PRIVATE_KEY"]
    # Render stores multiline env vars as \n — normalize to real newlines
    pem = pem.replace("\\n", "\n")
    return serialization.load_pem_private_key(
        pem.encode(),
        password=None,
        backend=default_backend()
    )


def sign_request(method, path):
    """
    Returns signed headers for a Kalshi API request.
    method: 'GET', 'POST', etc.
    path:   '/trade-api/v2/portfolio/fills' (no query string)
    """
    api_key     = os.environ["KALSHI_API_KEY"]
    private_key = load_private_key()

    timestamp_ms = str(int(time.time() * 1000))
    msg          = timestamp_ms + method.upper() + path

    signature = private_key.sign(
        msg.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )

    return {
        "KALSHI-ACCESS-KEY":       api_key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "Content-Type":            "application/json",
    }


def kalshi_get(path, params=None):
    """
    Authenticated GET request to Kalshi API.
    path: API path without base URL e.g. '/trade-api/v2/portfolio/fills'
    params: optional dict of query parameters
    """
    headers  = sign_request("GET", path)
    url      = f"https://api.elections.kalshi.com{path}"
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def test_auth():
    """Quick auth test — prints your Kalshi balance."""
    try:
        data = kalshi_get("/trade-api/v2/portfolio/balance")
        balance = data.get("balance", "unknown")
        print(f"Auth OK — balance: {balance} cents (${balance/100:.2f})")
        return True
    except Exception as e:
        print(f"Auth FAILED: {e}")
        return False


if __name__ == "__main__":
    test_auth()
