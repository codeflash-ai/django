"""
Serialize data to/from JSON
"""

import datetime
import decimal
import json
import uuid

from django.core.serializers.base import DeserializationError
from django.core.serializers.python import Deserializer as PythonDeserializer
from django.core.serializers.python import Serializer as PythonSerializer
from django.utils.duration import duration_iso_string
from django.utils.functional import Promise
from django.utils.timezone import is_aware


class Serializer(PythonSerializer):
    """Convert a queryset to JSON."""

    internal_use_only = False

    def _init_options(self):
        self._current = None
        self.json_kwargs = self.options.copy()
        self.json_kwargs.pop("stream", None)
        self.json_kwargs.pop("fields", None)
        if self.options.get("indent"):
            # Prevent trailing spaces
            self.json_kwargs["separators"] = (",", ": ")
        self.json_kwargs.setdefault("cls", DjangoJSONEncoder)
        self.json_kwargs.setdefault("ensure_ascii", False)

    def start_serialization(self):
        self._init_options()
        self.stream.write("[")

    def end_serialization(self):
        if self.options.get("indent"):
            self.stream.write("\n")
        self.stream.write("]")
        if self.options.get("indent"):
            self.stream.write("\n")

    def end_object(self, obj):
        # self._current has the field data
        indent = self.options.get("indent")
        if not self.first:
            self.stream.write(",")
            if not indent:
                self.stream.write(" ")
        if indent:
            self.stream.write("\n")
        json.dump(self.get_dump_object(obj), self.stream, **self.json_kwargs)
        self._current = None

    def getvalue(self):
        # Grandparent super
        return super(PythonSerializer, self).getvalue()


def Deserializer(stream_or_string, **options):
    """Deserialize a stream or string of JSON data."""
    if not isinstance(stream_or_string, (bytes, str)):
        stream_or_string = stream_or_string.read()
    if isinstance(stream_or_string, bytes):
        stream_or_string = stream_or_string.decode()
    try:
        objects = json.loads(stream_or_string)
        yield from PythonDeserializer(objects, **options)
    except (GeneratorExit, DeserializationError):
        raise
    except Exception as exc:
        raise DeserializationError() from exc


class DjangoJSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time, decimal types, and
    UUIDs.
    """

    def default(self, o):
        # See "Date Time String Format" in the ECMA-262 specification.
        typ = type(o)
        if typ is datetime.datetime:
            r = o.isoformat()
            if o.microsecond:
                # Only slice if the string is long enough and microsecond present
                # r usually looks like 2024-06-07T13:37:58.123456+00:00 or similar
                if len(r) >= 26:
                    r = r[:23] + r[26:]
            # Use slicing instead of removesuffix for fixed patterns
            if r.endswith("+00:00"):
                r = r[:-6] + "Z"
            return r
        elif typ is datetime.date:
            return o.isoformat()
        elif typ is datetime.time:
            if is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                # If microsecond is present, r will be at least 12 chars
                r = r[:12]
            return r
        elif typ is datetime.timedelta:
            return duration_iso_string(o)
        # Use tuple for multiple types check, but skip isinstance chain for common cases
        elif isinstance(o, (decimal.Decimal, uuid.UUID, Promise)):
            return str(o)
        else:
            return super().default(o)
