#!/usr/bin/python
import os, sys
from gzip import GzipFile
from datetime import datetime
import uuid
import __builtin__
import cStringIO
import fcntl

class WarcRecordWriter(object):
    '''wrapper around GzipFile that puts padding at the end
    of record. only have minimum interface for a file-like.'''
    def __init__(self, w):
        self.w = w
    def write(self, *args):
        self.w.write(*args)
    def close(self):
        self.w.write('\r\n'*4)
        # GzipFile in Python 2.6 does not flush underlining fileobj upon
        # close(). So I'm calling flush() on it first.
        self.w.flush()
        self.w.close()

class WarcWriter(object):
    def __init__(self, compresslevel=9, metadata={}, filename=None):
        self.filename = filename
        self.file = None
        self.compresslevel = int(compresslevel)
        self.metadata = dict(
            format='WARC File Format 1.0',
            conformsTo='http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf')
        self.metadata.update(**metadata)

    def startwarc(self):
        if self.filename is None:
            raise ValueError, 'filename is not supplied'
        self.file = __builtin__.open(self.filename + '.open', 'wb')
        fcntl.flock(self.file, fcntl.LOCK_EX)
        self.write_warcinfo()

    def finish_warc(self):
        '''close current WARC file, unlock and rename it, and resets
        file and filename. next call to get_record_writer will fail
        until new filename is supplied.'''
        if self.file:
            fcntl.flock(self.file, fcntl.LOCK_UN)
            self.file.close()
            try:
                os.rename(self.filename + '.open', self.filename)
            except:
                pass
            self.file = None
            self.filename = None
            
    def write_warcinfo(self):
        w = GzipFile(fileobj=self.file, mode='wb',
                     compresslevel=self.compresslevel)
        self.write_record(w, 'warcinfo', body=self.metadata)
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
            
    def start_record(self, w, type, clen, uri=None, digest=None, ip=None,
                     record_id=None):
        '''writes WARC record headers'''
        assert type in ('warcinfo', 'response', 'request', 'metadata'), \
            'bad type %s' % type
        content_type = dict(warcinfo='application/warc-fields',
                            response='application/http; msgtype=response',
                            request='application/http; msgtype=request',
                            metadata='application/warc-fields')[type]
        if record_id is None:
            record_id = '<url:uuid:%s>' % uuid.uuid1()
        w.write('WARC/1.0\r\n')
        w.write('WARC-Type: %s\r\n' % type)
        w.write('WARC-Date: %s\r\n' % datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
        if type == 'warcinfo':
            w.write('WARC-Filename: %s\r\n' % self.filename)
        w.write('WARC-Record-ID: %s\r\n' % record_id)
        w.write('Content-Type: %s\r\n' % content_type)
        if type == 'response' or type == 'request':
            if uri is not None:
                w.write('WARC-Target-URI: %s\r\n' % uri)
        if type == 'response':
            if digest is not None:
                w.write('WARC-Payload-Digest: %s\r\n' % digest)
            if ip is not None:
                w.write('WARC-IP-Address: %s\r\n' % ip)
        w.write('Content-Length: %d\r\n' % clen)
        w.write('\r\n')

    def write_record(self, w, type, body, **data):
        assert body is not None, 'data must have "body"'
        if isinstance(body, dict):
            body = ''.join("%s: %s\r\n" % item for item in body.iteritems())
        self.start_record(w, type, self.sizeof(body), **data)

        if isinstance(body, basestring):
            w.write(body)
        else:
            # TODO: copy data
            pass

        w.write('\r\n\r\n\r\n\r\n')
        
    def write_response(self, body, compresslevel=None, **kwds):
        w = self.get_record_writer(compresslevel=compresslevel)
        self.write_record(w, 'response', body=body, **kwds)

    def write_headers(self, w, headers):
        if headers:
            for k, v in headers.iteritems():
                if isinstance(v, basestring):
                    w.write('%s: %s\r\n' % (k, v))
                else:
                    for v1 in v:
                        w.write('%s: %s\r\n' % (k, v1))
        
    def start_response(self, headers, clen, statusline, compresslevel=None,
                       **kwds):
        '''start new response record, write out response headers, and
        return a writer for writing len bytes of response body.
        be sure to call close() on returned writer.'''
        w = self.get_record_writer(compresslevel=compresslevel)
        h = cStringIO.StringIO()
        self.write_headers(h, headers)
        hs = h.getvalue(); h.close()
        statusline = statusline.rstrip() + '\r\n'
        self.start_record(w, 'response', clen + len(statusline) + len(hs) + 2,
                          **kwds)
        w.write(statusline)
        w.write(hs)
        w.write('\r\n')
        return WarcRecordWriter(w)

    def start_request(self, headers, clen, requestline, compresslevel=None,
                      **kwds):
        '''start new request record, write out request headers, and
        return a writer for writing len bytes of request body.
        be sure to call close() on returned writer.'''
        w = self.get_record_writer(compresslevel=compresslevel)
        h = cStringIO.StringIO()
        self.write_headers(h, headers)
        hs = h.getvalue(); h.close()
        requestline = requestline.rstrip() + '\r\n'
        self.start_record(w, 'request', clen + len(requestline) + len(hs) + 2,
                          **kwds)
        w.write(requestline)
        w.write(hs)
        w.write('\r\n')
        return WarcRecordWriter(w)
        
    def close(self):
        self.finish_warc()

class RollingWarcWriter(WarcWriter):
    '''extension of WarcWriter that automatically roll over to next
    WARC file at predefined split size. Supply file name generator
    in fngen parameter.'''
    STANDARD_SPLIT_SIZE = 1024**3 # 1GB

    def __init__(self, compresslevel=9, metadata={},
                 fngen=None, splitsize=None, **kwds):
        WarcWriter.__init__(self, compresslevel, metadata)
        self.splitsize = splitsize or self.STANDARD_SPLIT_SIZE
        if fngen is None:
            def prefix_seq(prefix, seq=0):
                if prefix is None or prefix == '':
                    raise ValueError, 'prefix must be non-empty string'
                while 1:
                    yield '%s-%05d.warc.gz' % (prefix, seq)
                    seq += 1
            fngen = prefix_seq(kwds.get('prefix'))
        self.__fngen = fngen
    
    def makefilename(self):
        return next(self.__fngen)

    def check_size(self):
        if self.file and self.file.tell() > self.splitsize:
            self.finish_warc()

    # override
    def startwarc(self):
        self.filename = self.makefilename()
        WarcWriter.startwarc(self)

    # override
    def get_record_writer(self, compresslevel=None):
        self.check_size()
        return WarcWriter.get_record_writer(self, compresslevel)

def open(prefix, compresslevel=9, metadata={}):
    return RollingWarcWriter(compresslevel, metadata, prefix=prefix)
