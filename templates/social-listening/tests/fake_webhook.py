"""Local webhook receiver for the delivery tests.

    python3 tests/fake_webhook.py PORT FAIL_FIRST LOG_FILE

Answers HTTP 503 to the first FAIL_FIRST requests, then 200. Every request is
appended to LOG_FILE as one JSON line with its Idempotency-Key and status.
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT, FAIL_FIRST, LOG = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
state = {"n": 0}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        state["n"] += 1
        status = 503 if state["n"] <= FAIL_FIRST else 200
        with open(LOG, "a") as handle:
            handle.write(
                json.dumps(
                    {
                        "n": state["n"],
                        "status": status,
                        "key": self.headers.get("Idempotency-Key"),
                        "bytes": len(body),
                    }
                )
                + "\n"
            )
        self.send_response(status)
        self.end_headers()
        self.wfile.write(b"ok" if status == 200 else b"unavailable")

    def log_message(self, *args):
        pass


HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
