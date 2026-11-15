"""Forward real local runtime requests while controlling selected reply delivery."""

import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import socketserver
import sys
import threading
import time

configuration = json.loads(Path(sys.argv[1]).read_text())
rules = Path(configuration["rules"])
events = Path(configuration["events"])
lock = threading.Lock()
fired = set()


def record(event):
    with lock, events.open("a") as output:
        output.write(json.dumps(event) + "\n")
        output.flush()
        os.fsync(output.fileno())


class Upstream(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(configuration["timeoutMs"] / 1000)
        self.sock.connect(configuration["upstream"])


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle_request(self):
        rule = json.loads(rules.read_text())
        if rule.get("blockEvents") and self.path.startswith("/v1.56/events"):
            record({"kind": "blocked_observation", "method": self.command, "path": self.path})
            self.close_connection = True
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        upstream = Upstream("localhost", timeout=configuration["timeoutMs"] / 1000)
        upstream.request(self.command, self.path, body, {"Content-Type": "application/json"})
        response = upstream.getresponse()
        data = response.read()
        event = {"kind": "response", "method": self.command, "path": self.path, "status": response.status}
        record(event)
        should_hold = False
        if rule.get("method") == self.command and self.path.startswith(rule.get("path", "\0")):
            with lock:
                if rule["id"] not in fired:
                    fired.add(rule["id"])
                    should_hold = True
        if should_hold:
            event["body"] = data.decode()
            marker = Path(rule["marker"])
            pending = marker.with_name(marker.name + ".pending")
            with pending.open("x") as output:
                output.write(json.dumps(event))
                output.flush()
                os.fsync(output.fileno())
            pending.replace(marker)
            # The test owns the release signal; no provider response is invented.
            while not Path(rule["release"]).exists():
                time.sleep(configuration["pollMs"] / 1000)
        try:
            self.send_response(response.status)
            self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            record({"kind": "reply_lost", "method": self.command, "path": self.path})
            self.close_connection = True
        finally:
            upstream.close()

    do_GET = handle_request
    do_POST = handle_request
    do_DELETE = handle_request

    def log_message(self, *_):
        pass


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


server = Server(configuration["socket"], Handler)
Path(configuration["ready"]).write_text(str(os.getpid()))
server.serve_forever()
