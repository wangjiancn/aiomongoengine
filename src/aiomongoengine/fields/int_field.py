from .base_field import BaseField


class IntField(BaseField):
    """ Field responsible for storing integer values (:py:func:`int`).

    Available arguments (apart from those in `BaseField`):

    * `min_value` - Raises a validation error if the integer being stored is lesser than this value
    * `max_value` - Raises a validation error if the integer being stored is greather than this value
    """

    def __init__(self,
                 min_value=None,
                 max_value=None,
                 *args,
                 **kw):
        super().__init__(*args, **kw)
        self.min_value = min_value
        self.max_value = max_value

    def get_value(self, value) -> int:
        value = super().get_value(value)
        if value is None:
            return value
        return int(value)

    def to_son(self, value):
        if value is None:
            return None
        return int(value)

    def from_son(self, value):
        if value is None:
            return None
        return int(value)

    def validate(self, value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            self.error(f"`{value}` could not be converted to int")

        if self.min_value is not None and value < self.min_value:
            self.error("Integer value is too small")

        if self.max_value is not None and value > self.max_value:
            self.error("Integer value is too large")
