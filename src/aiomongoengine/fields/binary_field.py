from bson import Binary

from .base_field import BaseField


class BinaryField(BaseField):
    """ Field responsible for storing binary values. """

    def __init__(self, max_bytes: int = None, *args, **kwargs):
        """ Available arguments (apart from those in `BaseField`):

        :param max_bytes: The maximum number of bytes that can be stored in
            this field
        """
        super(BinaryField, self).__init__(*args, **kwargs)
        self.max_bytes = max_bytes

    def to_son(self, value):
        return Binary(value)

    def validate(self, value):
        if not isinstance(value, (str, Binary)):
            self.error("BinaryField only accepts instances of (str, Binary)")

        if self.max_bytes is not None and len(value) > self.max_bytes:
            self.error("Binary value is too long")

    def is_empty(self, value):
        return value is None or value == ""
