from typing import Union
from uuid import UUID

from .base_field import BaseField


class UUIDField(BaseField):
    """ Field responsible for storing :py:class:`uuid.UUID`. """

    def __init__(self, binary=True, *args, **kwargs):
        self._binary = binary
        super().__init__(*args, **kwargs)

    def validate(self, value):
        if not isinstance(value, UUID):
            try:
                UUID(value)
            except (ValueError, TypeError, AttributeError) as e:
                self.error(f"Could not convert to UUID: {e}")

    def is_empty(self, value) -> bool:
        return value is None or str(value) == ""

    def get_value(self, value) -> UUID:
        value = super().get_value(value)
        if isinstance(value, str):
            try:
                value = UUID(value)
            except (ValueError, TypeError, AttributeError):
                return value
        return value

    def to_son(self, value) -> Union[None, UUID]:
        if not self._binary:
            return str(value)
        elif isinstance(value, str):
            return UUID(value)
        return value

    def from_son(self, value) -> Union[None, UUID]:
        if not self._binary:
            original_value = value
            try:
                if not isinstance(value, str):
                    value = str(value)
                return UUID(value)
            except (ValueError, TypeError, AttributeError):
                return original_value
        return value
