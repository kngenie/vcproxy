"""
A http proxy based on the SocketServer Module
Author: Jonas Wagner

HTTPRipper a generic ripper for the web
Copyright (C) 2008-2009 Jonas Wagner

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sys
import os
import SocketServer
import socket
from select import select
from urlparse import urlparse
import logging
from collections import defaultdict
from cStringIO import StringIO

logger = logging

socket.setdefaulttimeout(30)


class HTTPProxyHandler(SocketServer.StreamRequestHandler):
    """handles a connection from the client, can handle multiple requests"""

    def parse_request(self):
        """parse a request line"""
        request = ""
        while not request:
            request = self.rfile.readline().strip()
        logger.debug("request %r", request)
        method, rawurl, version = request.split(" ")
        return method, rawurl, version

    def parse_header(self, f):
        """read the httpheaders from the file like f into a dictionary"""
        logger.debug("processing headers")
        headers = defaultdict(list)
        for line in f:
            if not line.strip():
                break
            key, value = line.split(": ", 1)
            headers[key].append(value.strip())
        return headers

    def write_headers(self, f, headers):
        """
        Forward the dictionary containing httpheaders *headers*
        to the file f. Writes a newline at the end.
        """
        logger.debug("forwarding headers %r", headers)
        for name, values in headers.items():
            for header in self.server.skip_headers:
                if name.startswith(header):
                    continue
            else:
                for value in values:
                    f.write("%s: %s\r\n" % (name, value))
        f.write("\r\n")

    def forward(self, f1, f2, maxlen=0):
        """forward maxlen bytes from f1 to f2"""
        logger.debug("forwarding %r bytes", maxlen)
        left = maxlen if maxlen is not None else sys.maxsize
        while left:
            data = f1.read(min(left, 1024))
            if not data:
                break
            f2.write(data)
            left -= len(data)

    forward_request_body = forward
    forward_response_body = forward

    def request_url(self, method, rawurl, version):
        """create a new socket and write the requestline"""
        url = urlparse(rawurl)
        self.requestline = "%s %s%s %s\r\n" % (method, url.path or "/",
                url.query and "?" + url.query or "", version)
        logging.debug("request_url(%r, %r, %r) request: %r", method, rawurl, version, self.requestline)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((url.hostname, int(url.port or 80)))
        s.sendall(self.requestline)
        return s, s.makefile("rwb", 0)

    def __repr__(self):
        return "HTTPProxyRequestHandler(%r)" % self.url

    def handle(self):
        try:
            self._handle()
        except:
            logger.exception("An error occured while handling request %r", self)
            raise

    def handle_connect(self):
        logging.debug('handle_connect %s', self.url)
        host, port = self.url.split(":")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((host, int(port)))
        logging.debug('opened socket to %s:%s', host, port)
        logging.debug('sending 200 response to the cilent')
        res = 'HTTP/1.0 200 connection established\r\n\r\n'
        self.request.sendall(res)
        nclosed = 0
        while nclosed < 2:
            r, w, x = select([self.request, s], [], [])
            for sock in r:
                data = sock.recv(1024)
                peer = s if sock == self.request else self.request
                if data == '':
                    logging.debug('closing %s', peer)
                    peer.shutdown(socket.SHUT_WR)
                    nclosed += 1
                else:
                    logging.debug('sending %d bytes to %s', len(data), peer)
                    peer.sendall(data)

    def _handle(self):
        """Handle client requests"""
        while True:
            method, url, version = self.parse_request()
            self.url = url
            self.requestheaders = self.parse_header(self.rfile)
            if method == 'CONNECT':
                self.handle_connect()
                continue
            self.requestheaders["Connection"] = ["close"]
            try:
                sock, request = self.request_url(method, url, version)
            except socket.error as ex:
                if ex.errno == os.errno.ETIMEDOUT:
                    # TODO: some implementation returns non-standard 599
                    # status.
                    res = 'HTTP/1.1 504 Gateway Timeout\r\n\r\n'
                    self.wfile.write(res)
                else:
                    res = 'HTTP/1.1 502 Bad Gateway\r\n\r\n'
                    self.wfile.write(res)
                break
            self.write_headers(request, self.requestheaders)
            if method in ("POST", "PUT") and "Content-Length" in self.requestheaders:
                self.forward_request_body(self.rfile, request,
                        int(self.requestheaders["Content-Length"][0]))
                sock.shutdown(socket.SHUT_WR)
            elif method in ('GET'):
                self.forward_request_body(self.rfile, StringIO(), 0)
                #sock.shutdown(socket.SHUT_WR)
            # forward status line
            self.statusline = request.readline()
            self.wfile.write(self.statusline)
            self.responseheaders = self.parse_header(request)
            self.write_headers(self.wfile, self.responseheaders)
            try:
                clen = int(self.responseheaders.get("Content-Length")[0])
            except (KeyError, TypeError, ValueError, IndexError):
                clen = None
            self.forward_response_body(request, self.wfile, clen)
            try:
                request.close()
                sock.shutdown(socket.SHUT_RD)
            except:
                pass
            sock.close()
            if self.requestheaders.get("Proxy-Connection") != ["keep-alive"]:
                break
            break


class HTTPProxyServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    timeout = 90
    request_queue_size = 10

    def __init__(self, addr, handler=HTTPProxyHandler):
        SocketServer.TCPServer.__init__(self, addr, handler)
        self.skip_headers = ["Proxy-"]

    def handle_error(self, request, addr):
        pass


class HTTPProxy2ProxyHandler(HTTPProxyHandler):

    def request_url(self, method, rawurl, version):
        """create a new socket and write the requestline"""
        request = "%s %s%s %s\r\n" % (method, rawurl, version)
        logging.debug("request_url(%r, %r, %r) request: %r", method, rawurl, version, request)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect(self.server.proxy_addr)
        s.sendall(request)
        return s, s.makefile("rwb", 0)


class HTTPProxy2ProxyServer(HTTPProxyServer):

    def __init__(self, addr, proxy_addr):
        HTTPServer.__init__(self, addr)
        self.skip_headers = ["Proxy-"]
        self.proxy_addr = proxy_addr


def make_http_proxy(addr):
    import urllib
    proxies = urllib.getproxies()
    try:
        proxy_url = proxies["http"]
    except KeyError:
        return HTTPProxyServer(addr)
    url = urlparse(proxy_url)
    return HTTPProxy2ProxyServer(addr, (url.hostname, url.port or 8080))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    server = HTTPProxyServer(("0.0.0.0", 8080))
    server.serve_forever()

