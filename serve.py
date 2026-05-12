#!/usr/bin/env python3
# /// script
# dependencies = ["watchdog"]
# ///
import os
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

ROOT = Path(__file__).parent
PORT = int(os.environ.get("PORT", 4000))

LIVE_RELOAD = b"""<script>
(function(){var es=new EventSource('/__reload');es.onmessage=function(){location.reload()};})();
</script>"""

MIME = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
    ".ico": "image/x-icon",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".svg": "image/svg+xml",
}

_clients: list[queue.Queue] = []
_lock = threading.Lock()


def broadcast():
    with _lock:
        dead = []
        for q in _clients:
            try:
                q.put_nowait(b"data: reload\n\n")
            except Exception:
                dead.append(q)
        for q in dead:
            _clients.remove(q)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/__reload":
            q: queue.Queue = queue.Queue()
            with _lock:
                _clients.append(q)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                self.wfile.write(b": connected\n\n")
                self.wfile.flush()
                while True:
                    try:
                        self.wfile.write(q.get(timeout=25))
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
            except Exception:
                pass
            finally:
                with _lock:
                    if q in _clients:
                        _clients.remove(q)
            return

        if path == "/":
            path = "/index.html"
        fp = (ROOT / path.lstrip("/")).resolve()
        if not str(fp).startswith(str(ROOT)):
            fp = ROOT / "index.html"
        if not fp.exists() or fp.is_dir():
            fp = ROOT / "index.html"

        try:
            body = fp.read_bytes()
        except Exception:
            self.send_error(404)
            return

        if fp.suffix == ".html":
            body = body.replace(b"</body>", LIVE_RELOAD + b"</body>", 1)

        self.send_response(200)
        self.send_header("Content-Type", MIME.get(fp.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


class Watcher(FileSystemEventHandler):
    def __init__(self):
        self._timer: threading.Timer | None = None
        self._mu = threading.Lock()

    def on_modified(self, event):
        if event.is_directory:
            return
        p = Path(event.src_path)
        if p.name.startswith(".") or p.suffix not in MIME:
            return
        with self._mu:
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(0.08, self._fire, args=[p.name])
            self._timer.start()

    def _fire(self, name: str):
        print(f"  ↺  {name} — reloading", flush=True)
        broadcast()


class ReuseServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    obs = Observer()
    obs.schedule(Watcher(), str(ROOT), recursive=True)
    obs.start()

    httpd = ReuseServer(("127.0.0.1", PORT), Handler)
    print(f"Serving http://localhost:{PORT}  (watching {ROOT})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        obs.stop()
        obs.join()
