from .base_field import BaseField


class DictField(BaseField):
    """ Field responsible for storing dict objects. """

    def validate(self, value):
        if not isinstance(value, dict):
            self.error("StringField only accepts dict values.")

    def is_empty(self, value) -> bool:
        return value is None or value == {}

    def to_son(self, value) -> dict:
        if not isinstance(value, dict):
            return dict(value)
        return value
