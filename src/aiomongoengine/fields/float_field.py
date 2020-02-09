from .int_field import IntField


class FloatField(IntField):
    """ Field responsible for storing float values (:py:func:`float`). """

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

    def to_son(self, value):
        if value is None:
            return None
        return float(value)

    def from_son(self, value):
        return self.to_son(value)

    def validate(self, value):
        if isinstance(value, int):
            try:
                value = float(value)
            except OverflowError:
                self.error("The value is too large to be converted to float")

        if not isinstance(value, float):
            self.error("FloatField only accepts float and integer values")

        if self.min_value is not None and value < self.min_value:
            self.error("Float value is too small")

        if self.max_value is not None and value > self.max_value:
            self.error("Float value is too large")
