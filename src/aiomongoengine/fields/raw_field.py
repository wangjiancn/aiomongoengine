import six

from .base_field import BaseField


class RawField(BaseField):
    ''' Field responsible for storing text. '''

    def __init__(self, *args, **kw):
        super(RawField, self).__init__(*args, **kw)

    def validate(self, value):
        return True

    def is_empty(self, value):
        return not bool(value)
