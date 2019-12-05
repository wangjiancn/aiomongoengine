import six

from .base_field import BaseField


class StringField(BaseField):
    ''' Field responsible for storing text. '''

    def __init__(self, max_length=None, choices=None, *args, **kw):
        super(StringField, self).__init__(*args, **kw)
        self.max_length = max_length
        self.choices = choices

    def validate(self, value):
        if value is None:
            return True

        is_string = isinstance(value, six.string_types)

        if not is_string:
            return False

        if self.choices and value not in self.choices:
            return False

        below_max_length = self.max_length is not None and len(
            value) <= self.max_length

        return self.max_length is None or below_max_length

    def is_empty(self, value):
        return value is None or value == ""
