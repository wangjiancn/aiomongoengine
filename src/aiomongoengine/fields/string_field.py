from re import compile

from .base_field import BaseField


class StringField(BaseField):
    """ Field responsible for storing text. """

    def __init__(self,
                 max_length=None,
                 min_length=None,
                 regex=None,
                 choices=None,
                 *args,
                 **kw):
        super().__init__(*args, **kw)
        self.max_length = max_length
        self.min_length = min_length
        self.regex = compile(regex) if regex else None
        self.choices = choices

    def get_value(self, value) -> str:
        value = super().get_value(value)
        if value is None:
            return value
        return str(value)

    def validate(self, value):
        if value is None:
            return

        if not isinstance(value, str):
            self.error("StringField only accepts string values.")
        if self.max_length is not None and len(value) > self.max_length:
            self.error("String value is too long")

        if self.min_length is not None and len(value) < self.min_length:
            self.error("String value is too short")

        if self.regex is not None and self.regex.match(value) is None:
            self.error("String value did not match validation regex")

    def is_empty(self, value):
        return value is None or value == ""
