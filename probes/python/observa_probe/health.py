from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


def start_server(state, port=8000):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/metrics':
                body, status, content_type = generate_latest(), 200, CONTENT_TYPE_LATEST
            elif self.path == '/health':
                body, status, content_type = b'ready' if state['ready'] else b'not ready', 200 if state['ready'] else 503, 'text/plain'
            else:
                body, status, content_type = b'not found', 404, 'text/plain'
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    return server
