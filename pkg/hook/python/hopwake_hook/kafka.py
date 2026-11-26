"""Reading Kafka headers the way the tap does, for services that consume
Kafka with confluent-kafka or kafka-python."""

from typing import Any, Iterable, Optional, Tuple, Union

Headers = Optional[Iterable[Tuple[str, Union[bytes, str, None]]]]


def header(headers: Headers, name: str, default: str = "") -> str:
    """The value of the first header called name, as text; default when there
    is none. Takes the (key, value) list both clients give, or a dict."""
    if not headers:
        return default
    items: Any = headers.items() if isinstance(headers, dict) else headers
    for key, value in items:
        if isinstance(key, bytes):
            key = key.decode("utf-8", "replace")
        if key != name:
            continue
        if value is None:
            return ""
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).decode("utf-8", "replace")
        return str(value)
    return default
