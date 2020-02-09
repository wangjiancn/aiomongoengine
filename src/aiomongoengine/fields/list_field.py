from aiomongoengine.errors import ValidationError

from .base_field import BaseField


class ListField(BaseField):
    """ Field responsible for storing :py:class:`list`. """

    def __init__(self, base_field=None, *args, **kw):
        super().__init__(*args, **kw)

        if not isinstance(base_field, BaseField):
            raise ValueError(
                f"The list field 'field' argument must be an instance of \
                BaseField, not '{str(base_field)}'.")

        if not self.default:
            self.default = lambda: []

        self._base_field = base_field

    def validate(self, value):
        errors = {}
        base_field_name = self._base_field.__class__.__name__

        if value is None:
            value = []

        if hasattr(value, "iteritems") or hasattr(value, "items"):
            sequence = value.items()
        else:
            sequence = enumerate(value)
        for k, v in sequence:
            if not self._base_field.is_empty(v):
                try:
                    self._base_field.validate(v)
                except ValidationError as e:
                    errors[k] = e.errors or e
                except (ValueError, AssertionError, AttributeError) as e:
                    errors[k] = e
            elif self._base_field.required:
                errors[k] = ValidationError(
                    f"Base field {base_field_name} is required")
        if errors:
            self.error(f"Invalid {base_field_name} {value}",
                       errors=errors)

    def is_empty(self, value):
        return value is None or value == []

    def to_son(self, value):
        return [self._base_field.to_son(i) for i in value]

    def from_son(self, value):
        if value is None:
            return []
        return [self._base_field.from_son(i) for i in value]

    def to_query(self, value):
        if not isinstance(value, (tuple, set, list)):
            return value

        return {
            "$all": value
        }

    @property
    def item_type(self):
        if hasattr(self._base_field, 'embedded_type'):
            return self._base_field.embedded_type

        if hasattr(self._base_field, 'reference_type'):
            return self._base_field.reference_type

        return type(self._base_field)
