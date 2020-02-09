from .base_field import BaseField


class BooleanField(BaseField):
    """ Field responsible for storing boolean values (:py:func:`bool`). """

    def validate(self, value):
        if not isinstance(value, bool):
            self.error("BooleanField only accepts boolean values")
