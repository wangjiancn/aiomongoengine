from typing import Union
from typing import Callable
from typing import Any


class BaseField(object):
    """This class is the base to all fields. This is not supposed to be used \
    directly in documents.
    """

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
        self.__dict__.update(kwargs)

    def is_empty(self, value):
        """Indicates that the field is empty

        the default is comparing the value to None
        """
        return value is None

    def get_value(self, value):
        return value

    def to_son(self, value):
        """Converts the value to the BSON representation required by motor."""
        return value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance._data.get(self.name)

    def __set__(self, instance, value):
        if value is None:
            if self.null:
                value = None
            elif self.default is not None:
                value = self.default() if callable(
                    self.default) else self.default
        instance._data[self.name] = value

    def to_query(self, value):
        return self.to_son(value)

    def from_son(self, value):
        """Parses the value from the BSON representation returned from motor."""
        return value

    def validate(self, value):
        """Returns if the specified value for the field is valid."""
        return True
