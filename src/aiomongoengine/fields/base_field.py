from typing import Any
from typing import Callable
from typing import NoReturn
from typing import Union

from aiomongoengine.errors import ValidationError


class BaseField(object):
    """This class is the base to all fields. This is not supposed to be used \
    directly in documents.
    """
    name: str = None

    total_creation_counter = 0

    def __init__(
            self,
            db_field: str = None,
            default: Union[Any, Callable] = None,
            required: bool = False,
            null: bool = False,
            on_save: Callable = None,
            unique: bool = False,
            unique_with: str = None,
            sparse: bool = False,
            **kwargs
    ):
        """

        :param db_field: The name this field will have when sent to MongoDB
        :param default: The default value (or callable) that will be used when
            first creating an instance that has no value set for the field
        :param required:Indicates that if the field value evaluates to empty
            (using the `is_empty` method) a validation error is raised
        :param on_save:A function of the form `lambda doc, creating` that is
            called right before sending the document to the DB.
        :param unique: Indicates whether an unique index should be created for
            this field.
        :param unique_with: Indicates whether a sparse index should be created
            for this field. This also will not pass empty values to DB.
        :param sparse: Indicates whether a sparse index should be created for
            this field. This also will not pass empty values to DB.
        """
        self.creation_counter = BaseField.total_creation_counter
        BaseField.total_creation_counter += 1

        self.db_field = db_field
        self.required = required
        self.default = default
        self.on_save = on_save
        self.unique = unique
        self.sparse = sparse
        self.unique_with = unique_with
        self.null = null
        self._owner_document = None
        self.__dict__.update(kwargs)

    def get_value(self, value) -> Any:
        """ Get field's value from document. """
        if value is None:
            if self.null:
                value = None
            elif self.default is not None:
                if callable(self.default):
                    value = self.default()
                else:
                    value = self.default
        return value

    def get_db_prep_value(self, value) -> Any:
        """ Return field's value prepare for save into a database. """
        return self.get_value(value)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = instance._data.get(self.db_field)
        return self.get_value(value)

    def __set__(self, instance, value):
        instance._data[self.db_field] = self.get_value(value)

    def is_empty(self, value) -> bool:
        """Indicates that the field is empty

        the default is comparing the value to None
        """
        return value is None

    def to_son(self, value) -> Any:
        """Converts the value to the BSON representation required by motor."""
        return value

    def from_son(self, value) -> Any:
        """Parses the value from the BSON representation returned from motor."""
        return self.to_son(value)

    def to_query(self, value) -> Any:
        """ Return field's for queryset. """
        return self.to_son(value)

    def validate(self, value) -> NoReturn:
        """ Validate field's value, raise `ValidationError` when valid. """
        pass

    def error(self, message="", errors=None, field_name=None) -> NoReturn:
        """ Raise a ValidationError."""
        field_name = field_name or self.name or self.db_field
        raise ValidationError(message, errors=errors, field_name=field_name)

    @property
    def owner_document(self):
        return self._owner_document

    def _set_owner_document(self, owner_document):
        self._owner_document = owner_document

    @owner_document.setter
    def owner_document(self, owner_document):
        self._set_owner_document(owner_document)
