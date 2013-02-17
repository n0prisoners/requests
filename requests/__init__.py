"""
Poor man's fork of Requests at <http://www.python-requests.org>

"""
__title__ = 'requests'
__version__ = '0.0.1'
__build__ = 0x010100
__author__ = 'Kenneth Reitz, Paul Malyschko'
__licence__ = 'Apache 2.0'
__copyright__ = 'Copyright 2013 Kenneth Reitz, Paul Malyschko'

import os
import platform

def detect_platform():
    _implementation = None
    _implementation_version = None

    if 'SERVER_SOFTWARE' in os.environ:
        v = os.environ['SERVER_SOFTWARE']
        if v.startswith('Google App Engine/') or v.startswith('Development/'):
            _implementation = 'Google App Engine'
        else:
            _implementation = platform.python_implementation()
    else:
        _implementation = platform.python_implementation()

    if _implementation == 'CPython':
        _implementation_version = platform.python_version()
    elif _implementation == 'PyPy':
        _implementation_version = '%s.%s.%s' % (sys.pypy_version_info.major,
            sys.pypy_version_info.minor, sys.pypy_version_info.micro)
        if sys.pypy_version_info.releaselevel != 'final':
            _implementation_version = ''.join(
                [_implementation_version, sys.pypy_version_info.releaselevel])
    elif _implementation == 'Jython':
        _implementation_version = platform.python_version()  # Complete Guess
    elif _implementation == 'IronPython':
        _implementation_version = platform.python_version()  # Complete Guess
    elif _implementation == 'Google App Engine':
        v = os.environ['SERVER_SOFTWARE']
        _implementation_version = v.split('/')[1]
    else:
        _implementation_version = 'Unknown'

    return {
        'implementation': _implementation,
        'version': _implementation_version
    }

PLATFORM = detect_platform()

import cgi
import httplib
import json
import socket
import threading
import urllib
import urllib2
import zlib
import StringIO

from urlparse import urlparse, urlunparse
from collections import OrderedDict

if PLATFORM['implementation'] == 'Google App Engine':
    from google.appengine.api import urlfetch
    from google.appengine.api.urlfetch import InvalidURLError
    from google.appengine.api.urlfetch import DownloadError
    from google.appengine.api.urlfetch import ResponseTooLargeError
    from google.appengine.api.urlfetch import SSLCertificateError
    from google.appengine.runtime import DeadlineExceededError

CONTENT_CHUNK_SIZE = 10 * 1024
ITER_CHUNK_SIZE = 10 * 1024

try:
    import ssl
except:
    pass


"""
Structures

"""

class CaseInsensitiveDict(dict):
    """Case-insensitive Dictionary

    For example, ``headers['content-encoding']`` will return the
    value of a ``'Content-Encoding'`` response header."""

    @property
    def lower_keys(self):
        if not hasattr(self, '_lower_keys') or not self._lower_keys:
            self._lower_keys = dict((k.lower(), k) for k in list(self.keys()))
        return self._lower_keys

    def _clear_lower_keys(self):
        if hasattr(self, '_lower_keys'):
            self._lower_keys.clear()

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self._clear_lower_keys()

    def __delitem__(self, key):
        dict.__delitem__(self, self.lower_keys.get(key.lower(), key))
        self._lower_keys.clear()

    def __contains__(self, key):
        return key.lower() in self.lower_keys

    def __getitem__(self, key):
        # We allow fall-through here, so values default to None
        if key in self:
            return dict.__getitem__(self, self.lower_keys[key.lower()])

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default


"""
Utils

"""

def from_key_val_list(value):
    """Take an object and test to see if it can be represented as a
    dictionary. Unless it can not be represented as such, return an
    OrderedDict, e.g.,

    ::

        >>> from_key_val_list([('key', 'val')])
        OrderedDict([('key', 'val')])
        >>> from_key_val_list('string')
        ValueError: need more than 1 value to unpack
        >>> from_key_val_list({'key': 'val'})
        OrderedDict([('key', 'val')])
    """
    if value is None:
        return None

    if isinstance(value, (basestring, bytes, bool, int)):
        raise ValueError('cannot encode objects that are not 2-tuples')

    return OrderedDict(value)

def get_encoding_from_headers(headers):
    """Returns encodings from given HTTP Header Dict.

    :param headers: dictionary to extract encoding from.
    """

    content_type = headers.get('content-type')

    if not content_type:
        return None

    content_type, params = cgi.parse_header(content_type)

    if 'charset' in params:
        return params['charset'].strip("'\"")

    if 'text' in content_type:
        return 'ISO-8859-1'

def stream_decode_response_unicode(iterator, r):
    """Stream decodes a iterator."""

    if r.encoding is None:
        for item in iterator:
            yield item
        return

    decoder = codecs.getincrementaldecoder(r.encoding)(errors='replace')
    for chunk in iterator:
        rv = decoder.decode(chunk)
        if rv:
            yield rv
    rv = decoder.decode('', final=True)
    if rv:
        yield rv

def iter_slices(string, slice_length):
    """Iterate over slices of a string."""
    pos = 0
    while pos < len(string):
        yield string[pos:pos + slice_length]
        pos += slice_length

def stream_decompress(iterator, mode='gzip'):
    """Stream decodes an iterator over compressed data

    :param iterator: An iterator over compressed data
    :param mode: 'gzip' or 'deflate'
    :return: An iterator over decompressed data
    """

    if mode not in ['gzip', 'deflate']:
        raise ValueError('stream_decompress mode must be gzip or deflate')

    zlib_mode = 16 + zlib.MAX_WBITS if mode == 'gzip' else -zlib.MAX_WBITS
    dec = zlib.decompressobj(zlib_mode)
    try:
        for chunk in iterator:
            rv = dec.decompress(chunk)
            if rv:
                yield rv
    except zlib.error:
        # If there was an error decompressing, just return the raw chunk
        yield chunk
        # Continue to return the rest of the raw data
        for chunk in iterator:
            yield chunk
    else:
        # Make sure everything has been returned from the decompression object
        buf = dec.decompress(bytes())
        rv = buf + dec.flush()
        if rv:
            yield rv

def stream_untransfer(gen, resp):
    ce = resp.headers.get('content-encoding', '').lower()
    if 'gzip' in ce:
        gen = stream_decompress(gen, mode='gzip')
    elif 'deflate' in ce:
        gen = stream_decompress(gen, mode='deflate')

    return gen

def default_user_agent():
    """Return a string representing the default user agent."""
    _implementation = platform.python_implementation()

    if _implementation == 'CPython':
        _implementation_version = platform.python_version()
    elif _implementation == 'PyPy':
        _implementation_version = '%s.%s.%s' % (sys.pypy_version_info.major,
            sys.pypy_version_info.minor, sys.pypy_version_info.micro)
        if sys.pypy_version_info.releaselevel != 'final':
            _implementation_version = ''.join(
                [_implementation_version, sys.pypy_version_info.releaselevel])
    elif _implementation == 'Jython':
        _implementation_version = platform.python_version()  # Complete Guess
    elif _implementation == 'IronPython':
        _implementation_version = platform.python_version()  # Complete Guess
    else:
        _implementation_version = 'Unknown'

    try:
        p_system = platform.system()
        p_release = platform.release()
    except IOError:
        p_system = 'Unknown'
        p_release = 'Unknown'

    return " ".join([
            'python-requests/%s' % __version__,
            '%s/%s' % (_implementation, _implementation_version),
            '%s/%s' % (p_system, p_release),
        ])

def default_headers():
    return {
        'User-Agent': default_user_agent(),
        'Accept-Encoding': ', '.join(('gzip', 'deflate', 'compress')),
        'Accept': '*/*'
    }

def parse_header_links(value):
    """Return a dict of parsed link headers proxies.

    i.e. Link: <http:/.../front.jpeg>; rel=front;
    type="image/jpeg",<http://.../back.jpeg>; rel=back;type="image/jpeg"

    """

    links = []
    replace_chars = " '\""

    for val in value.split(","):
        try:
            url, params = val.split(";", 1)
        except ValueError:
            url, params = val, ''

        link = {}
        link["url"] = url.strip("<> '\"")

        for param in params.split(";"):
            try:
                key,value = param.split("=")
            except ValueError:
                break

            link[key.strip(replace_chars)] = value.strip(replace_chars)
        links.append(link)

    return links

# Null bytes; no need to recreate these on each call to guess_json_utf
_null = '\x00'.encode('ascii')  # encoding to ASCII for Python 3
_null2 = _null * 2
_null3 = _null * 3

def guess_json_utf(data):
    # JSON always starts with two ASCII characters, so detection is as
    # easy as counting the nulls and from their location and count
    # determine the encoding. Also detect a BOM, if present.
    sample = data[:4]
    if sample in (codecs.BOM_UTF32_LE, codecs.BOM32_BE):
        return 'utf-32'     # BOM included
    if sample[:3] == codecs.BOM_UTF8:
        return 'utf-8-sig'  # BOM included, MS style (discouraged)
    if sample[:2] in (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
        return 'utf-16'     # BOM included
    nullcount = sample.count(_null)
    if nullcount == 0:
        return 'utf-8'
    if nullcount == 2:
        if sample[::2] == _null2:   # 1st and 3rd are null
            return 'utf-16-be'
        if sample[1::2] == _null2:  # 2nd and 4th are null
            return 'utf-16-le'
        # Did not detect 2 valid UTF-16 ascii-range characters
    if nullcount == 3:
        if sample[:3] == _null3:
            return 'utf-32-be'
        if sample[1:] == _null3:
            return 'utf-32-le'
        # Did not detect a valid UTF-32 ascii-range character
    return None


def prepend_scheme_if_needed(url, new_scheme):
    '''Given a URL that may or may not have a scheme, prepend the given scheme.
    Does not replace a present scheme with the one provided as an argument.'''
    scheme, netloc, path, params, query, fragment = urlparse(url, new_scheme)

    # urlparse is a finicky beast, and sometimes decides that there isn't a
    # netloc present. Assume that it's being over-cautious, and switch netloc
    # and path if urlparse decided there was no netloc.
    if not netloc:
        netloc, path = path, netloc

    return urlunparse((scheme, netloc, path, params, query, fragment))


"""
Exceptions

"""

class RequestException(Exception):
    """There was an ambiguous exception that occurred while handling your
    request."""

class HTTPError(RequestException):
    """An HTTP error occurred."""
    response = None

class ConnectionError(RequestException):
    """A Connection error occured."""

class SSLError(ConnectionError):
    """An SSL error occurred."""

class Timeout(RequestException):
    """The request timed out."""

class MissingSchema(RequestException, ValueError):
    """The URL schema (e.g. http or https) is missing."""

class InvalidSchema(RequestException, ValueError):
    """See defaults.py for valid schemas."""

class InvalidURL(RequestException, ValueError):
    """The URL provided was somehow invalid."""

class ResponseTooLarge(RequestException):
    """The response from the server was too large."""

"""
Models

"""

class Request(object):
    """HTTP request"""
    
    def __init__(self,
        method=None,
        url=None,
        headers=None,
        data=None,
        callback=None):
        
        data = '' if not data else data
        headers = {} if not headers else headers
        
        self.method = method
        self.url = url
        self.headers = headers
        self.data = data
        self.callback = callback
    
    def __repr__(self):
        return '<Request [%s]>' % (self.method)

class Response(object):
    """Server response to HTTP request"""
    
    def __init__(self):
        self._content = False
        self._content_consumed = False
        
        self.status_code = None
        self.raw = None
        self.url = None
        self.encoding = None
        self.reason = None
    
    def __repr__(self):
        return '<Response [%s]>' % (self.status_code)
    
    def __bool__(self):
        return self.ok
    
    def __nonzero__(self):
        return self.ok
    
    def __iter__(self):
        return self.iter_content(128)
    
    @property
    def ok(self):
        try:
            self.raise_for_status()
        except RequestException:
            return False
        return True
    
    @property
    def apparent_encoding(self):
        return 'utf-8'
    
    def iter_content(self, chunk_size=1, decode_unicode=False):
        if self._content_consumed:
            return iter_slices(self._content, chunk_size)
        
        def generate():
            f = None
            if PLATFORM['implementation'] != 'Google App Engine':
                f = self.raw
            else:
                f = StringIO.StringIO(self.raw.content)
            while 1:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
            self._content_consumed = True
        
        gen = stream_untransfer(generate(), self)
        
        if decode_unicode:
            gen = stream_decode_response_unicode(gen, self)
        
        return gen
    
    def iter_lines(self, chunk_size=ITER_CHUNK_SIZE):
        pending = None
        
        for chunk in self.iter_content(
            chunk_size=chunk_size,
            decode_unicode=decode_unicode):
            if pending:
                chunk = pending + chunk
            lines = chunk.splitlines()
            
            if lines and lines[-1] and chunk and lines[-1][-1] is chunk[-1]:
                pending = lines.pop()
            else:
                pending = None
            
            for line in lines:
                yield line
            
        if pending:
            yield pending
    
    @property
    def content(self):
        if self._content is False:
            try:
                if self._content_consumed:
                    raise Exception(
                        'The content for this response was already consumed')
                
                if self.status_code is 0:
                    self._content = None
                else:
                    self._content = bytes().join(self.iter_content(
                        CONTENT_CHUNK_SIZE)) or bytes()
            except AttributeError:
                self._content = None
        
        self._content_consumed = True
        return self._content
    
    @property
    def text(self):
        content = None
        encoding = self.encoding
        
        if not self.content:
            return unicode('')
        
        if not self.encoding:
            encoding = self.apparent_encoding
        
        try:
            content = unicode(self.content, encoding, errors='replace')
        except (LookupError, TypeError):
            content = unicode(self.content, errors='replace')
        
        return content
    
    @property
    def json(self):
        if not self.encoding and len(self.content) > 3:
            encoding = guess_json_utf(self.content)
            if not encoding:
                return json.loads(self.content.decode(encoding))
        return json.loads(self.text or self.content)
    
    @property
    def links(self):
        header = self.headers['link']
        l = {}
        
        if header:
            links = parse_header_links(header)
            for link in links:
                key = link.get('rel') or link.get('url')
                l[key] = link
        
        return l
    
    def raise_for_status(self):
        http_error_msg = ''
        
        if 400 <= self.status_code < 500:
            http_error_msg = '%s Client Error: %s' % (self.status_code,
                self.reason)
        elif 500 <= self.status_code < 600:
            http_error_msg = '%s Server Error: %s' % (self.status_code, 
                self.reason)

        if http_error_msg:
            http_error = HTTPError(http_error_msg)
            http_error.response = self
            raise http_error
    
    def close(self):
        pass


"""
Adapters

"""

class VerifiedHTTPSConnection(httplib.HTTPSConnection):
    def connect(self):
        sock = socket.create_connection((self.host, self.port), self.timeout)
        
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        
        self.sock = ssl.wrap_socket(sock,
            self.key_file,
            self.cert_file,
            cert_reqs=ssl.CERT_REQUIRED,
            ca_certs=os.path.join(os.path.dirname(__file__), 'cacert.pem'))

class VerifiedHTTPSHandler(urllib2.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(VerifiedHTTPSConnection, req)

class BaseConnection(object):
    def send(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError
    
    def wait(self):
        raise NotImplementedError

class BaseAdapter(object):
    def send(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError

class DefaultConnection(BaseConnection):
    def build_handler(self, url, verify=True):
        scheme = urlparse(url).scheme
        handler = None
        
        if scheme == 'https':
            if verify:
                handler = VerifiedHTTPSHandler()
            else:
                handler = urllib2.HTTPSHandler()
        elif scheme == 'http':
            handler = urllib2.HTTPHandler()
        elif len(scheme):
            raise InvalidSchema("Invalid scheme: %s" % (scheme))
        else:
            raise MissingSchema("Missing scheme")
        
        return handler
    
    def build_request(self, request):
        req = urllib2.Request(request.url)
        for k, v in request.headers.items():
            req.add_header(k, v)
        req.get_method = lambda: request.method
        return req
    
    def build_response(self, req, resp):
        response = Response()
        response.status_code = resp.getcode()
        response.headers = CaseInsensitiveDict(getattr(resp, 'headers', {}))
        response.encoding = get_encoding_from_headers(response.headers)
        response.raw = resp
        
        if isinstance(req.get_full_url(), bytes):
            response.url = req.get_full_url().decode('utf-8')
        else:
            response.url = req.get_full_url()
        
        response.request = req
        return response
    
    def open(self, opener, request, data, timeout, callback=None):
        e = None
        r = None
        
        try:
            response = opener.open(request, data, timeout)
        except ssl.SSLError:
            e = SSLError("SSL error")
        except socket.error as error:
            e = ConnectionError("Connection error")
        except socket.timeout as error:
            e = Timeout("Connection timed out")
        except IOError as error:
            if hasattr(error, 'reason'):
                if error.reason == "no host given":
                    e = InvalidURL("Invalid URL")
                elif "unknown url type:" in error.reason:
                    scheme = error.reason.split("'")[1]
                    
                    if len(scheme):
                        e = InvalidSchema("Invalid scheme: %s" % (scheme))
                    else:
                        e = MissingSchema("Missing scheme")
                else:
                    e = RequestException(error.reason)
            elif hasattr(error, 'code'):
                self.raise_for_status()
        except AttributeError:
            e = SSLError("SSL not supported on this platform")
        except Exception as error:
            e = RequestException(str(error))
        else:
            r = self.build_response(request, response)
            r.content
        
        if callback:
            callback(r, e)
        else:
            if e is not None:
                raise e
            else:
                return r
    
    def send(self, request, timeout=None, verify=True, callback=None):
        request.url = prepend_scheme_if_needed(request.url, 'https')
        data = None
        
        if request.data:
            if request.method == 'POST' or request.method == 'PUT':
                data = request.data
            elif request.method == 'GET':
                request.url = ''.join([
                    request.url,
                    '?',
                    urllib.urlencode(request.data)])
        
        req = self.build_request(request)
        handler = self.build_handler(request.url, verify)
        opener = urllib2.build_opener(handler)
        
        if callback:
            args = (opener, req, data, timeout, callback)
            self.thread = threading.Thread(target=self.open, args=args)
            return self
        else:
            return self.open(opener, req, data, timeout)
    
    def close(self):
        pass
    
    def wait(self):
        if not self.thread.is_alive():
            self.thread.start()

class DefaultAdapter(BaseAdapter):
    """Default adapter for urllib2"""
    
    def __init__(self):
        self.rpc = DefaultConnection()
    
    def send(self, request, timeout=None, verify=True, callback=None):
        return self.rpc.send(request, timeout, verify, callback)
    
    def close(self):
        return self.rpc.close()

class AppEngineConnection(BaseConnection):
    def build_response(self, req, resp):
        response = Response()
        response.status_code = getattr(resp, 'status_code', None)
        response.headers = CaseInsensitiveDict(getattr(resp, 'headers', {}))
        response.encoding = get_encoding_from_headers(response.headers)
        response.raw = resp

        if isinstance(req.url, bytes):
            response.url = req.url.decode('utf-8')
        else:
            response.url = req.url

        response.request = req
        return response

    def build_callback(self, rpc, request, callback=None):
        def wrapper():
            r = None
            e = None

            try:
                r = self.build_response(request, rpc.get_result())
            except InvalidURLError as error:
                scheme, netloc, path, params, query, fragment = urlparse(url)
                
                if not len(scheme):
                    e = MissingSchema("Missing scheme")
                elif scheme != 'http' or scheme != 'https':
                    e = InvalidSchema("Invalid scheme: %s" % (scheme))
                else:
                    e = InvalidURL("Invalid URL")
            except DownloadError:
                e = ConnectionError("Connection error")
            except ResponseTooLargeError:
                e = ResponseTooLarge("Response too large")
            except SSLCertificateError:
                e = SSLError("SSL certificate invalid")
            except DeadlineExceededError:
                e = Timeout("Connection timed out")
            except urlfetch.Error as error:
                e = RequestException(str(e))
            else:
                try:
                    r.content
                    r.raise_for_status()
                except HTTPError as error:
                    e = error
            
            if callback:
                callback(r, e)
        
        return wrapper
    
    def send(self, request, timeout=None, verify=True, callback=None):
        url = prepend_scheme_if_needed(request.url, 'http')
        data = None
        
        if request.method == 'POST' or 'PUT':
            data = request.data
        elif request.method == 'GET':
            url = ''.join([url, '?', urllib.urlencode(request.data)])
        
        if callback:
            self.rpc = urlfetch.create_rpc()
            self.rpc.timeout = timeout
            self.rpc.callback = self.build_callback(self.rpc,
                request,
                callback)
            
            urlfetch.make_fetch_call(self.rpc,
                url,
                payload=data,
                method=request.method,
                headers=request.headers,
                validate_certificate=verify)
            return self
        else:
            try:
                response = urlfetch.fetch(url,
                    payload=data,
                    method=request.method,
                    headers=request.headers,
                    deadline=timeout,
                    validate_certificate=verify)
            except InvalidURLError:
                scheme, netloc, path, params, query, fragment = urlparse(url)
                
                if not len(scheme):
                    raise MissingSchema("Missing scheme")
                elif scheme != 'http' or scheme != 'https':
                    raise InvalidSchema("Invalid scheme: %s" % (scheme))
                else:
                    raise InvalidURL("Invalid URL")
            except DownloadError:
                raise ConnectionError("Connection error")
            except ResponseTooLargeError:
                raise ResponseTooLarge("Response too large")
            except SSLCertificateError:
                raise SSLError("SSL certificate invalid")
            except DeadlineExceededError:
                raise Timeout("Connection timed out")
            except urlfetch.Error as e:
                raise RequestException(str(e))
            else:
                r = self.build_response(request, response)
                try:
                    r.content
                    r.raise_for_status()
                except HTTPError as e:
                    raise e
                else:
                    return r
    
    def close(self):
        pass
    
    def wait(self):
        self.rpc.wait()

class AppEngineAdapter(BaseAdapter):
    """Adapter for Google AppEngine"""
    
    def __init__(self):
        self.rpc = AppEngineConnection()
    
    def send(self, request, timeout=None, verify=True, callback=None):
        return self.rpc.send(request, timeout, verify, callback)
    
    def close(self):
        return self.rpc.close()

"""
Sessions

"""

def merge_kwargs(local_kwarg, default_kwarg):
    """Merges kwarg dictionaries.

    If a local key in the dictionary is set to None, it will be removed.
    """

    if default_kwarg is None:
        return local_kwarg

    if isinstance(local_kwarg, basestring):
        return local_kwarg

    if local_kwarg is None:
        return default_kwarg

    # Bypass if not a dictionary (e.g. timeout)
    if not hasattr(default_kwarg, 'items'):
        return local_kwarg

    default_kwarg = from_key_val_list(default_kwarg)
    local_kwarg = from_key_val_list(local_kwarg)

    # Update new values in a case-insensitive way
    def get_original_key(original_keys, new_key):
        """
        Finds the key from original_keys that case-insensitive matches
        new_key.
        """
        for original_key in original_keys:
            if key.lower() == original_key.lower():
                return original_key
        return new_key

    kwargs = default_kwarg.copy()
    original_keys = kwargs.keys()
    for key, value in local_kwarg.items():
        kwargs[get_original_key(original_keys, key)] = value

    # Remove keys that are set to None.
    for (k, v) in local_kwarg.items():
        if v is None:
            del kwargs[k]

    return kwargs

class Session(object):
    def __init__(self):
        self.headers = default_headers()
        self.verify = True
        self.adapters = {}
        
        if PLATFORM['implementation'] != 'Google App Engine':
            self.mount('http://', DefaultAdapter())
            self.mount('https://', DefaultAdapter())
        else:
            self.mount('http://', AppEngineAdapter())
            self.mount('https://', AppEngineAdapter())
    
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.close()
    
    def request(self, method, url,
        data=None,
        headers=None,
        timeout=None,
        verify=None,
        callback=None):
        headers = merge_kwargs(headers, self.headers)
        verify = merge_kwargs(verify, self.verify)
        
        req = Request()
        req.method = method.upper()
        req.url = url
        req.headers = {} if not headers else headers
        req.data = {} if not data else data
        req.callback = callback
        
        return self.send(req,
            timeout=timeout,
            verify=verify, 
            callback=callback)
        
    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)
    
    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)
    
    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)
    
    def delete(self, url, **kwargs):
        return self.request('DELETE', url, **kwargs)
    
    def send(self, request, **kwargs):
        adapter = self.get_adapter(request.url)
        r = adapter.send(request, **kwargs)
        return r
    
    def get_adapter(self, url):
        for (prefix, adapter) in self.adapters.items():
            if url.startswith(prefix):
                return adapter
        
        scheme, netloc, path, params, query, fragment = urlparse(url)
        raise InvalidSchema("Invalid scheme: %s" % (scheme))
    
    def close(self):
        for v in self.adapters.values():
            v.close()
    
    def mount(self, prefix, adapter):
        self.adapters[prefix] = adapter
    
    def __getstate__(self):
        return dict((attr, getattr(self, attrNone)) for attr in 
            self.__attrs__)
    
    def __setstate__(self, state):
        for attr, value in state.items():
            setattr(self, attr, value)


"""
API

"""

def request(method, url, **kwargs):
    session = Session()
    return session.request(method, url, **kwargs)

def get(url, **kwargs):
    return request('GET', url, **kwargs)

def post(url, **kwargs):
    return request('POST', url, **kwargs)

def put(url, **kwargs):
    return request('PUT', url, **kwargs)

def delete(url, **kwargs):
    return request('DELETE', url, **kwargs)