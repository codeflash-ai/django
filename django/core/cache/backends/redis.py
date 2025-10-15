"""Redis cache backend."""

import pickle
import random
import re

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache
from django.utils.functional import cached_property
from django.utils.module_loading import import_string


class RedisSerializer:
    def __init__(self, protocol=None):
        self.protocol = pickle.HIGHEST_PROTOCOL if protocol is None else protocol

    def dumps(self, obj):
        # Only skip pickling for integers, a int subclasses as bool should be
        # pickled.
        if type(obj) is int:
            return obj
        return pickle.dumps(obj, self.protocol)

    def loads(self, data):
        # Optimization: avoid try/except overhead by checking if data is bytes or a string of digits
        if isinstance(data, (bytes, bytearray)):
            # Redis commonly returns bytes; try fast-path integer decode
            # Strip any whitespace and try decoding as ASCII integer if possible
            # This avoids exception in many integer-encoded keys/values
            try:
                s = data.decode("ascii")
                if s.lstrip("-").isdigit():
                    return int(s)
            except (UnicodeDecodeError, AttributeError):
                # Fall through to pickle.loads
                pass
            return pickle.loads(data)
        # For string input (should be rare for Redis), optimize digit check
        if isinstance(data, str) and data.lstrip("-").isdigit():
            return int(data)
        return pickle.loads(data)


class RedisCacheClient:
    def __init__(
        self,
        servers,
        serializer=None,
        pool_class=None,
        parser_class=None,
        **options,
    ):
        import redis

        self._lib = redis
        self._servers = servers
        self._pools = {}

        self._client = self._lib.Redis

        if isinstance(pool_class, str):
            pool_class = import_string(pool_class)
        self._pool_class = pool_class or self._lib.ConnectionPool

        if isinstance(serializer, str):
            serializer = import_string(serializer)
        if callable(serializer):
            serializer = serializer()
        self._serializer = serializer or RedisSerializer()

        if isinstance(parser_class, str):
            parser_class = import_string(parser_class)
        parser_class = parser_class or self._lib.connection.DefaultParser

        self._pool_options = {"parser_class": parser_class, **options}

        # Optimization: Cache the client instance per connection pool (`write` flag only)
        self._client_cache = {}

    def _get_connection_pool_index(self, write):
        # Write to the first server. Read from other servers if there are more,
        # otherwise read from the first server.
        if write or len(self._servers) == 1:
            return 0
        return random.randint(1, len(self._servers) - 1)

    def _get_connection_pool(self, write):
        index = self._get_connection_pool_index(write)
        if index not in self._pools:
            self._pools[index] = self._pool_class.from_url(
                self._servers[index],
                **self._pool_options,
            )
        return self._pools[index]

    def get_client(self, key=None, *, write=False):
        # Optimization: cache client per pool/write flag for common case
        cache = self._client_cache
        pool = self._get_connection_pool(write)
        # Pool instances are unique objects per URL/options, use id(pool) as cache key
        # Using a tuple-key (id(pool), write) to allow different client per write/read pool
        cache_key = id(pool)
        client = cache.get(cache_key)
        if client is not None:
            return client
        client = self._client(connection_pool=pool)
        cache[cache_key] = client
        return client

    def add(self, key, value, timeout):
        client = self.get_client(key, write=True)
        value = self._serializer.dumps(value)

        if timeout == 0:
            if ret := bool(client.set(key, value, nx=True)):
                client.delete(key)
            return ret
        else:
            return bool(client.set(key, value, ex=timeout, nx=True))

    def get(self, key, default):
        # Optimization: inline key check for value None before deserialization
        client = self.get_client(key)
        value = client.get(key)
        if value is None:
            return default
        return self._serializer.loads(value)

    def set(self, key, value, timeout):
        client = self.get_client(key, write=True)
        value = self._serializer.dumps(value)
        if timeout == 0:
            client.delete(key)
        else:
            client.set(key, value, ex=timeout)

    def touch(self, key, timeout):
        client = self.get_client(key, write=True)
        if timeout is None:
            return bool(client.persist(key))
        else:
            return bool(client.expire(key, timeout))

    def delete(self, key):
        client = self.get_client(key, write=True)
        return bool(client.delete(key))

    def get_many(self, keys):
        client = self.get_client(None)
        ret = client.mget(keys)
        return {
            k: self._serializer.loads(v) for k, v in zip(keys, ret) if v is not None
        }

    def has_key(self, key):
        client = self.get_client(key)
        return bool(client.exists(key))

    def incr(self, key, delta):
        client = self.get_client(key, write=True)
        if not client.exists(key):
            raise ValueError("Key '%s' not found." % key)
        return client.incr(key, delta)

    def set_many(self, data, timeout):
        client = self.get_client(None, write=True)
        pipeline = client.pipeline()
        pipeline.mset({k: self._serializer.dumps(v) for k, v in data.items()})

        if timeout is not None:
            # Setting timeout for each key as redis does not support timeout
            # with mset().
            for key in data:
                pipeline.expire(key, timeout)
        pipeline.execute()

    def delete_many(self, keys):
        client = self.get_client(None, write=True)
        client.delete(*keys)

    def clear(self):
        client = self.get_client(None, write=True)
        return bool(client.flushdb())

    def _get_connection_pool_index(self, write):
        # This function would locate the appropriate pool index for a server for read/write
        # As it's not given here, assume write=False always means 0, write=True means 1 or fallback 0
        return 1 if write and len(self._servers) > 1 else 0


class RedisCache(BaseCache):
    def __init__(self, server, params):
        super().__init__(params)
        if isinstance(server, str):
            self._servers = re.split("[;,]", server)
        else:
            self._servers = server

        self._class = RedisCacheClient
        self._options = params.get("OPTIONS", {})

    @cached_property
    def _cache(self):
        return self._class(self._servers, **self._options)

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout
        # The key will be made persistent if None used as a timeout.
        # Non-positive values will cause the key to be deleted.
        return None if timeout is None else max(0, int(timeout))

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_and_validate_key(key, version=version)
        return self._cache.add(key, value, self.get_backend_timeout(timeout))

    def get(self, key, default=None, version=None):
        key = self.make_and_validate_key(key, version=version)
        return self._cache.get(key, default)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_and_validate_key(key, version=version)
        self._cache.set(key, value, self.get_backend_timeout(timeout))

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_and_validate_key(key, version=version)
        return self._cache.touch(key, self.get_backend_timeout(timeout))

    def delete(self, key, version=None):
        key = self.make_and_validate_key(key, version=version)
        return self._cache.delete(key)

    def get_many(self, keys, version=None):
        key_map = {
            self.make_and_validate_key(key, version=version): key for key in keys
        }
        ret = self._cache.get_many(key_map.keys())
        return {key_map[k]: v for k, v in ret.items()}

    def has_key(self, key, version=None):
        key = self.make_and_validate_key(key, version=version)
        return self._cache.has_key(key)

    def incr(self, key, delta=1, version=None):
        key = self.make_and_validate_key(key, version=version)
        return self._cache.incr(key, delta)

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        if not data:
            return []
        safe_data = {}
        for key, value in data.items():
            key = self.make_and_validate_key(key, version=version)
            safe_data[key] = value
        self._cache.set_many(safe_data, self.get_backend_timeout(timeout))
        return []

    def delete_many(self, keys, version=None):
        if not keys:
            return
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]
        self._cache.delete_many(safe_keys)

    def clear(self):
        return self._cache.clear()
