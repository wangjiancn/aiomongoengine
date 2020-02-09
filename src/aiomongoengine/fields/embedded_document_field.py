from aiomongoengine import get_collections

from .base_field import BaseField


class EmbeddedDocumentField(BaseField):
    """
    Field responsible for storing an embedded document.

    Available arguments (apart from those in `BaseField`):

    * `embedded_document_type` - The type of document that this field accepts as an embedded document.
    """

    def __init__(self, embedded_document_type=None, *args, **kw):
        super(EmbeddedDocumentField, self).__init__(*args, **kw)

        self.embedded_document_type = embedded_document_type

    @property
    def embedded_type(self):
        if isinstance(self.embedded_document_type, str):
            self.embedded_document_type = get_collections().get(
                self.embedded_document_type)

        return self.embedded_document_type

    def validate(self, value):
        # avoiding circular reference
        from ..document import Document

        if not isinstance(self.embedded_type, type) or not issubclass(
                self.embedded_type, Document):
            self.error(
                "The field 'embedded_document_type' argument must be a "
                "subclass of Document, not '%s'." % str(self.embedded_type))

        if value is not None and not isinstance(value, self.embedded_type):
            embedded_type_name = self.embedded_type.__class__.__name__
            self.error(f"Value must be the instance of {embedded_type_name}")

        value.validate()

    def to_son(self, value):
        if value is None:
            return None

        base = dict()
        base.update(value.to_son())
        return base

    def from_son(self, value):
        if value is None:
            return None
        return self.embedded_type.from_son(value)
