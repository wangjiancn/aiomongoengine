from bson.objectid import ObjectId

from .base_field import BaseField


class ObjectIdField(BaseField):
    """
    Field responsible for storing object ids.

    Usage:

    .. testcode:: modeling_fields

        objectid = ObjectIdField(required=True)
    """

    def validate(self, value):
        return value is None or isinstance(value, ObjectId)

    def get_value(self, value):
        if isinstance(value, str):
            return ObjectId(str)
        elif isinstance(value, ObjectId):
            return value

    def to_son(self, value):
        if isinstance(value, str):
            return ObjectId(value)
        else:
            return value
