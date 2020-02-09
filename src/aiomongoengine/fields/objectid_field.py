from bson.errors import InvalidId
from bson.objectid import ObjectId

from .base_field import BaseField


class ObjectIdField(BaseField):
    """ Field responsible for storing object ids. """

    def validate(self, value):
        if not isinstance(value, ObjectId):
            try:
                ObjectId(value)
            except (TypeError, InvalidId) as e:
                self.error(f"Could not convert to ObjectId {e}")

    def get_value(self, value):
        value = super().get_value(value)
        if isinstance(value, str):
            return ObjectId(value)
        elif isinstance(value, ObjectId):
            return value
