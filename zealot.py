import bsddb
from datetime import datetime, timedelta
import gzip
import hashlib
import os

class Datastore(object):

    '''
    Expects a full directory, use os.path.abspath if necessary
    '''
    
    def __init__(self, directory):

        if not os.path.exists(directory):
            os.makedirs(directory)
            
        self._directory = directory
        self._db = bsddb.btopen(os.path.join(directory, 'index.db'))
        self._date_format = "%Y-%m-%dT%H:%M:%S"
        
    def _date_to_string(self, date):
        return date.strftime(self._date_format)

    def _string_to_date(self, string):
        return datetime.strptime(string, self._date_format)
    
    def _get(self, key, callback, expires_after):

        results = self._db.get(key)

        if results is not None:
            filename, expiration = results.split(';')
            expiration = self._string_to_date(expiration)
            
        if results is None or datetime.now() > expiration:
            value = callback()
            self._put(key, value, expires_after)
        else:
            with gzip.open(os.path.join(self._directory, filename), 'rb') as f:
                value = f.read()

        return value

    def _put(self, key, value, expires_after):

        filename = hashlib.sha512(value).digest().encode('hex')
        
        with gzip.open(os.path.join(self._directory, filename), 'wb') as f:
            f.write(value)

        self._db[key] = filename + ';' + self._date_to_string(expires_after)
        
    def cache(self, expires_after):
        def wrapper(f):
            def inner(*args, **kwargs):
                key = f.__name__ + ' ' + repr((args, tuple(kwargs.items())))
                return self._get(key, lambda: f(*args, **kwargs), expires_after)
            return inner
        return wrapper

if __name__ == "__main__":

    import requests
    
    cache_dir = os.path.join(os.path.dirname(__file__), 'test')

    zstore = Datastore(cache_dir)
    expire = datetime.now() + timedelta(minutes=10)
    
    @zstore.cache(expire)
    def get_html(url):
        return requests.get(url).content

    get_html(r'https://google.com')
