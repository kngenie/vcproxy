#!/usr/bin/python
import os, sys
from gzip import GzipFile
from datetime import datetime
import uuid
import __builtin__
import cStringIO

class WarcWriter(object):
    SPLIT_SIZE = 1024**3 # 1GB

    def __init__(self, prefix, compresslevel=9, metadata={}):
        self.prefix = prefix
        if self.prefix is None or self.prefix == '':
            raise ValueError, 'prefix must not be None or empty string'
        self.seq = 0
        self.filename = None
        self.file = None
        self.compresslevel = int(compresslevel)
        self.metadata = dict(
            format='WARC File Format 1.0',
            conformsTo='http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf')
        self.metadata.update(**metadata)

    def startwarc(self):
        self.filename = '%s-%05d.warc.gz' % (self.prefix, self.seq)
        self.file = __builtin__.open(self.filename + '.open', 'wb')
        self.write_warcinfo()

    def finish_warc(self):
        if self.file:
            self.file.close()
            try:
                os.rename(self.filename + '.open', self.filename)
            except:
                pass
            self.file = None
            self.filename = None
            
    def check_size(self):
        p = self.file.tell()
        if p > self.SPLIT_SIZE:
            self.finish_warc()
            self.seq += 1

    def write_warcinfo(self):
        w = GzipFile(fileobj=self.file, mode='wb',
                     compresslevel=self.compresslevel)
        self.write_record(w, 'warcinfo', dict(body=self.metadata))
        w.close() # does not close underlining file
        self.file.flush()

    def get_record_writer(self, compresslevel=None):
        if self.file is None:
            self.startwarc()
        if compresslevel is None: compresslevel = self.compresslevel
        return GzipFile(fileobj=self.file, mode='wb',
                        compresslevel=compresslevel)
    
    def sizeof(self, data):
        if isinstance(data, basestring):
            return len(data)
        if hasattr(data, 'fileno'):
            return os.fstat(data.fileno).st_size
            
    def start_record(self, w, type, clen, data):
        assert type in ('warcinfo', 'response', 'request', 'metadata'), \
            'bad type %s' % type
        content_type = dict(warcinfo='application/warc-fields',
                            response='application/http; msgtype=response',
                            request='application/http; msgtype=request',
                            metadata='application/warc-fields')[type]
        rid = '<url:uuid:%s>' % uuid.uuid1()
        w.write('WARC/1.0\r\n')
        w.write('WARC-Type: %s\r\n' % type)
        w.write('WARC-Date: %s\r\n' % datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
        if type == 'warcinfo':
            w.write('WARC-Filename: %s\r\n' % self.filename)
        w.write('WARC-Record-ID: %s\r\n' % rid)
        w.write('Content-Type: %s\r\n' % content_type)
        if type == 'response' or type == 'request':
            if 'uri' in data:
                w.write('WARC-Target-URI: %s\r\n' % data['uri'])
        if type == 'response':
            if 'digest' in data:
                w.write('WARC-Payload-Digest: %s\r\n' % data['digest'])
            if 'ip' in data:
                w.write('WARC-IP-Address: %s\r\n' % data['ip'])
        w.write('Content-Length: %d\r\n' % clen)
        w.write('\r\n')

    def write_record(self, w, type, data):
        body = data.get('body')
        assert body is not None, 'data must have "body"'
        if isinstance(body, dict):
            body = ''.join("%s: %s\r\n" % item for item in body.iteritems())
        self.start_record(w, type, self.sizeof(body), data)

        if isinstance(body, basestring):
            w.write(body)
        else:
            # TODO: copy data
            pass

        w.write('\r\n\r\n\r\n\r\n')
        
    def write_response(self, body, compresslevel=None, **kwds):
        data = dict(body=body)
        data.update(**kwds)
        w = self.get_record_writer(compresslevel=compresslevel)
        self.write_record(w, 'response', data)

    def write_headers(self, w, headers):
        if headers:
            for k, v in headers.iteritems():
                if isinstance(v, basestring):
                    w.write('%s: %s\r\n' % (k, v))
                else:
                    for v1 in v:
                        w.write('%s: %s\r\n' % (k, v1))
        
    def start_response(self, headers, clen, compresslevel=None, **kwds):
        '''start new response record, write out response headers, and
        return a writer for writing len bytes of response body.
        be sure to call close() on returned writer.'''
        w = self.get_record_writer(compresslevel=compresslevel)
        h = cStringIO.StringIO()
        self.write_headers(h, headers)
        hs = h.getvalue(); h.close()
        self.start_record(w, 'response', clen + len(hs) + 2, kwds)
        w.write(hs)
        w.write('\r\n')
        return w

    def start_request(self, headers, clen, compresslevel=None, **kwds):
        '''start new request record, write out request headers, and
        return a writer for writing len bytes of request body.
        be sure to call close() on returned writer.'''
        w = self.get_record_writer(compresslevel=compresslevel)
        h = cStringIO.StringIO()
        self.write_headers(h, headers)
        hs = h.getvalue(); h.close()
        self.start_record(w, 'request', clen + len(hs) + 2, kwds)
        w.write(hs)
        w.write('\r\n')
        return w
        
    def close(self):
        self.finish_warc()

def open(prefix, compresslevel=9, metadata={}):
    return WarcWriter(prefix, compresslevel, metadata)
