#!/usr/bin/env python3
"""Local dev server that serves static files and proxies Met Museum images."""
import sys, ssl, urllib.request
from http.server import HTTPServer, SimpleHTTPRequestHandler

class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/metimg/'):
            url = 'https://images.metmuseum.org/' + self.path[8:]
            try:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
                })
                with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
                    data = resp.read()
                    self.send_response(200)
                    self.send_header('Content-Type', resp.headers.get('Content-Type', 'image/jpeg'))
                    self.send_header('Cache-Control', 'public, max-age=86400')
                    self.end_headers()
                    self.wfile.write(data)
            except Exception as e:
                self.send_error(502, str(e))
        else:
            super().do_GET()

    def log_message(self, fmt, *args):
        pass  # silence logs

port = int(sys.argv[1]) if len(sys.argv) > 1 else 8742
print(f"Serving on http://localhost:{port}")
HTTPServer(('', port), Handler).serve_forever()
