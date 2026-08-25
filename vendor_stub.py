#!/usr/bin/env python3
"""
Yuktee.ai — Technical Assignment, Part 2
Simulated messaging vendor.

This stands in for an external messaging provider (think WhatsApp BSP, SMS
gateway). It is deliberately unreliable. Every candidate gets the SAME sequence
of failures, so your submission is compared against everyone else's on identical
conditions.

RUN IT
    python3 vendor_stub.py
    (Python 3.8+, standard library only, nothing to install.)

    Listens on http://localhost:9000

CALL IT
    POST /send
    Content-Type: application/json

    {
      "lead_id":        "lead-123",          # required
      "idempotency_key": "any-stable-string", # optional, but see below
      "message":        "Hi, calling about your enquiry"
    }

WHAT COMES BACK
    200  {"status":"sent","vendor_message_id":"vm_...","duplicate":false}
    200  {"status":"sent","vendor_message_id":"vm_...","duplicate":true}
            -> the vendor accepted this a second time. If you did not send it
               twice, we did. Your job is to make sure the LEAD is not
               messaged twice.
    429  {"error":"rate_limited","retry_after":2}
    503  {"error":"service_unavailable"}
    ---  no response at all: the connection hangs for 30s, then closes.
            Set your own timeout. Do not wait 30 seconds.

    There is also a sustained outage in the sequence: a stretch of consecutive
    503s lasting longer than a naive retry loop will tolerate. Handling the
    blip and handling the outage are different problems.

IDEMPOTENCY
    If you send an "idempotency_key" the vendor MOSTLY honours it and returns
    the original vendor_message_id with "duplicate": true. Mostly. It is an
    unreliable vendor. Assume it can fail to deduplicate and make sure the lead
    still is not messaged twice.

USEFUL ENDPOINTS
    GET  /_stats   what the vendor thinks happened: every call, every response,
                   and how many distinct sends each lead received. Use this to
                   check your own work.
    POST /_reset   clear state and restart the sequence from the beginning.

    Prefix "_" endpoints are for you while developing. Your service should only
    ever call /send.

NOTE
    The sequence is fixed and seeded. Restarting the stub replays the same
    failures in the same order. Do not hard-code around the sequence — we run
    submissions against a different seed.
"""

import json
import random
import threading
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- config

PORT = 9000
SEED = 20260819          # changed when we evaluate submissions
HANG_SECONDS = 30        # how long a "timeout" response hangs before closing
OUTAGE_START = 12        # nth call at which the sustained outage begins
OUTAGE_LENGTH = 9        # number of consecutive 503s in the outage
DEDUP_FAILURE_RATE = 0.15  # chance the vendor forgets it saw this key before

# Weighted outcomes for ordinary (non-outage) calls.
OUTCOMES = (
    ["ok"] * 5 +
    ["rate_limited"] * 2 +
    ["unavailable"] * 2 +
    ["timeout"] * 2
)

# ---------------------------------------------------------------- state

_lock = threading.Lock()
_rng = random.Random(SEED)
_call_no = 0
_by_key = {}                      # idempotency_key -> vendor_message_id
_sends_per_lead = defaultdict(int)  # lead_id -> distinct accepted sends
_log = []


def _reset():
    global _rng, _call_no, _by_key, _sends_per_lead, _log
    _rng = random.Random(SEED)
    _call_no = 0
    _by_key = {}
    _sends_per_lead = defaultdict(int)
    _log = []


def _next_outcome():
    """Decide what this call does. Caller must hold _lock."""
    global _call_no
    _call_no += 1
    n = _call_no
    if OUTAGE_START <= n < OUTAGE_START + OUTAGE_LENGTH:
        return n, "unavailable"
    return n, _rng.choice(OUTCOMES)


# ---------------------------------------------------------------- handler

class Vendor(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # quiet; use /_stats instead

    # -- helpers ---------------------------------------------------

    def _json(self, code, payload):
        raw = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return None

    # -- routes ----------------------------------------------------

    def do_GET(self):
        if self.path == "/_stats":
            with _lock:
                self._json(200, {
                    "calls_received": _call_no,
                    "distinct_sends_per_lead": dict(_sends_per_lead),
                    "leads_messaged_more_than_once": [
                        k for k, v in _sends_per_lead.items() if v > 1
                    ],
                    "log": _log[-100:],
                })
            return
        if self.path in ("/", "/health"):
            self._json(200, {"vendor": "stub", "ok": True})
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path == "/_reset":
            with _lock:
                _reset()
            self._json(200, {"reset": True})
            return

        if self.path != "/send":
            self._json(404, {"error": "not_found"})
            return

        body = self._read_body()
        if body is None:
            self._json(400, {"error": "malformed_json"})
            return
        lead_id = body.get("lead_id")
        if not lead_id:
            self._json(400, {"error": "lead_id_required"})
            return
        key = body.get("idempotency_key")

        with _lock:
            n, outcome = _next_outcome()

            # Idempotent replay, when the vendor remembers.
            if outcome == "ok" and key and key in _by_key:
                if _rng.random() >= DEDUP_FAILURE_RATE:
                    _log.append({"call": n, "lead": lead_id, "result": "200 duplicate"})
                    self._json(200, {
                        "status": "sent",
                        "vendor_message_id": _by_key[key],
                        "duplicate": True,
                    })
                    return
                # else: the vendor forgot. Falls through and sends again.
                _log.append({"call": n, "lead": lead_id, "result": "dedup_missed"})

            if outcome == "ok":
                vmid = "vm_%06d" % _rng.randrange(10 ** 6)
                if key:
                    _by_key[key] = vmid
                _sends_per_lead[lead_id] += 1
                _log.append({"call": n, "lead": lead_id, "result": "200 sent"})
                self._json(200, {
                    "status": "sent",
                    "vendor_message_id": vmid,
                    "duplicate": False,
                })
                return

            if outcome == "rate_limited":
                _log.append({"call": n, "lead": lead_id, "result": "429"})
                self.send_response(429)
                raw = json.dumps({"error": "rate_limited", "retry_after": 2}).encode()
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "2")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
                return

            if outcome == "unavailable":
                _log.append({"call": n, "lead": lead_id, "result": "503"})
                self._json(503, {"error": "service_unavailable"})
                return

            # timeout
            _log.append({"call": n, "lead": lead_id, "result": "hang"})

        # Hang OUTSIDE the lock so other requests still get served.
        time.sleep(HANG_SECONDS)
        try:
            self.close_connection = True
        except Exception:
            pass


if __name__ == "__main__":
    _reset()
    print("Yuktee vendor stub listening on http://localhost:%d" % PORT)
    print("  POST /send      the vendor")
    print("  GET  /_stats    what actually happened")
    print("  POST /_reset    start the sequence over")
    ThreadingHTTPServer(("127.0.0.1", PORT), Vendor).serve_forever()
