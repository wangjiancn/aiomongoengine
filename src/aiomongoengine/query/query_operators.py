from re import compile
from re import I
from typing import Pattern

from .base import add_query_operator
from .base import QueryOperator


@add_query_operator
class IRegexOperator(QueryOperator):
    """Regex support """
    op = 'iregex'

    def to_query(self, field_name, value, template=""):
        if isinstance(value, str):
            if template:
                raw = template.format(value)
            else:
                raw = value
            pattern = compile(raw, I)
        elif issubclass(value, Pattern):
            pattern = value
        else:
            raise ValueError('unsupport Regex')
        return {
            field_name: {"$regex": pattern}
        }


@add_query_operator
class RegexOperator(QueryOperator):
    """Regex support """
    op = 'regex'

    def to_query(self, field_name, value, template=""):
        if isinstance(value, str):
            if template:
                raw = template.format(value)
            else:
                raw = value
            pattern = compile(raw)
        elif issubclass(value, Pattern):
            pattern = value
        else:
            raise ValueError('unsupport Regex')
        return {
            field_name: {"$regex": pattern}
        }


@add_query_operator
class IContainsOperator(IRegexOperator):
    op = 'icontains'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value)


@add_query_operator
class ContainsOperator(RegexOperator):
    op = 'contains'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value)


@add_query_operator
class IEndsWithOperator(IRegexOperator):
    op = 'iendswith'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value, template=r'{}$')


@add_query_operator
class EndsWithOperator(RegexOperator):
    op = 'endswith'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value, template=r'{}$')


@add_query_operator
class IExactOperator(IRegexOperator):
    op = 'iexact'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value, template=r'^{}$')


@add_query_operator
class ExactOperator(RegexOperator):
    op = 'exact'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(
            field_name, value, template=r'^{}$')


@add_query_operator
class ExistsQueryOperator(QueryOperator):
    op = 'exists'

    def to_query(self, field_name, value):
        return {field_name: {"$exists": value}}

    def get_value(self, field, value):
        return value


@add_query_operator
class GreaterThanQueryOperator(QueryOperator):
    op = 'gt'

    def to_query(self, field_name, value):
        return {field_name: {"$gt": value}}


@add_query_operator
class GreaterThanOrEqualQueryOperator(QueryOperator):
    op = 'gte'

    def to_query(self, field_name, value):
        return {field_name: {"$gte": value}}


@add_query_operator
class IStartsWithOperator(IRegexOperator):
    op = 'istartswith'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value, template=r'^{}')


@add_query_operator
class StartsWithOperator(RegexOperator):
    op = 'startswith'

    def to_query(self, field_name, value, **kwargs):
        return super().to_query(field_name, value, template=r'^{}')


@add_query_operator
class InQueryOperator(QueryOperator):
    op = 'in'

    def to_query(self, field_name, value):
        return {field_name: {"$in": value}}

    def get_value(self, field, value):
        return [field.to_son(val) for val in value]


@add_query_operator
class IsNullQueryOperator(QueryOperator):
    op = 'is_null'

    def to_query(self, field_name, value):
        if value:
            return {
                field_name: None
            }
        else:
            return {
                field_name: {
                    "$exists": True,
                    "$ne": None
                }
            }

    def get_value(self, field, value):
        return value


@add_query_operator
class LesserThanQueryOperator(QueryOperator):
    op = 'lt'

    def to_query(self, field_name, value):
        return {
            field_name: {"$lt": value}
        }


@add_query_operator
class LesserThanOrEqualQueryOperator(QueryOperator):
    op = 'lte'

    def to_query(self, field_name, value):
        return {
            field_name: {"$lte": value}
        }


@add_query_operator
class NotEqualQueryOperator(QueryOperator):
    op = 'ne'

    def to_query(self, field_name, value):
        return {
            field_name: {"$ne": value}
        }


@add_query_operator
class NotOperator(QueryOperator):
    op = 'not'

    def to_query(self, field_name, operator, value):
        result = operator.to_query(field_name, value, )
        return {field_name: {"$not": result[field_name]}}

    def get_value(self, field, value):
        return value
