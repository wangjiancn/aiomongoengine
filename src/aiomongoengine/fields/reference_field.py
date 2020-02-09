from typing import TYPE_CHECKING
from typing import Union

from aiomongoengine.utils import _import_class
from bson.objectid import ObjectId

from .base_field import BaseField
from ..connection import get_collections

if TYPE_CHECKING:
    from ..document import Document

RECURSIVE_REFERENCE_CONSTANT = "self"


class ReferenceField(BaseField):
    """ Field responsible for creating a reference to another document. """

    def __init__(self,
                 document_type_obj: Union[str, 'Document'],
                 *args, **kw):
        """

        :param document_type_obj: The type of document that this field
            accepts as a referenced document.
        """

        super(ReferenceField, self).__init__(*args, **kw)
        if isinstance(document_type_obj, str):
            pass
        elif not issubclass(document_type_obj, _import_class('Document')):
            self.error(
                "Argument to ReferenceField constructor must be a "
                "document class or a string"
            )

        self.document_type_obj = document_type_obj

    @property
    def reference_type(self):
        if isinstance(self.document_type_obj, str):
            if self.document_type_obj == RECURSIVE_REFERENCE_CONSTANT:
                self.document_type_obj = self.owner_document
            else:
                self.document_type_obj = get_collections().get(
                    self.document_type_obj)
        return self.document_type_obj

    def validate(self, value):
        if value is not None and not isinstance(value, (
                self.reference_type, ObjectId)):
            self.error("ReferenceField only accepts ObjectId or Document.")

        if isinstance(value, _import_class('Document')) and value.id is None:
            self.error("You can only reference documents once they have been "
                       "saved to the database")

    def to_son(self, value):
        if value is None:
            return None
        if isinstance(value, ObjectId):
            return value
        if isinstance(value, _import_class('Document')):
            return value.id
        return value

    def from_son(self, value):
        return value
