import re
import socket
import hashlib
import hmac
import six
import base64

try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc



def base64_encodestring(s):
    if six.PY2:
        return base64.encodestring(s)
    else:
        if isinstance(s, str):
            s = s.encode('utf8')
        return base64.encodebytes(s).decode('utf8')


class Util(object):
    @staticmethod
    def is_row_ip(ip):
        ipArr = ip.split('.')
        if len(ipArr) != 4:
            return False
        for tmp in ipArr:
            if not tmp.isdigit() or int(tmp) >= 256:
                return False
        pattern = re.compile(r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$')
        if pattern.match(ip):
            return True
        return False

    @staticmethod
    def get_host_ip(logHost):
        """ If it is not match your local ip, you should fill the PutLogsRequest
        parameter source by yourself.
        """
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((logHost, 80))
            ip = s.getsockname()[0]
            return ip
        except Exception:
            return '127.0.0.1'
        finally:
            if s:
                s.close()

    @staticmethod
    def compress_data(data):
        import zlib
        return zlib.compress(data, 6)

    @staticmethod
    def cal_md5(content):
        return hashlib.md5(content).hexdigest().upper()

    @staticmethod
    def hmac_sha1(content, key):
        if isinstance(content, six.text_type):  # hmac.new accept 8-bit str
            content = content.encode('utf-8')
        if isinstance(key, six.text_type):  # hmac.new accept 8-bit str
            key = key.encode('utf-8')

        hashed = hmac.new(key, content, hashlib.sha1).digest()
        return base64_encodestring(hashed).rstrip()

    @staticmethod
    def h_v_td(header, key, default):
        """
        get header value with title with default value
        try to get key from header and consider case sensitive
        e.g. header['x-log-abc'] or header['X-Log-Abc']
        :param header:
        :param key:
        :param default:
        :return:
        """
        if key not in header:
            key = key.title()

        return header.get(key, default)

    @staticmethod
    def convert_unicode_to_str(data):
        """
        Py2, always translate to utf8 from unicode
        Py3, always translate to unicode
        :param data:
        :return:
        """
        if six.PY2 and isinstance(data, six.text_type):
            return data.encode('utf8')
        elif six.PY3 and isinstance(data, six.binary_type):
            return data.decode('utf8')
        elif isinstance(data, collections_abc.Mapping):
            return dict((Util.convert_unicode_to_str(k), Util.convert_unicode_to_str(v))
                        for k, v in six.iteritems(data))
        elif isinstance(data, collections_abc.Iterable) and not isinstance(data, (six.binary_type, six.string_types)):
            return type(data)(map(Util.convert_unicode_to_str, data))

