import collections

from ..query.base import QueryOperator
from ..query.exists import ExistsQueryOperator
from ..query.greater_than import GreaterThanQueryOperator
from ..query.greater_than_or_equal import GreaterThanOrEqualQueryOperator
from ..query.lesser_than import LesserThanQueryOperator
from ..query.lesser_than_or_equal import LesserThanOrEqualQueryOperator
from ..query.in_operator import InQueryOperator
from ..query.is_null import IsNullQueryOperator
from ..query.not_operator import NotOperator
from ..query.not_equal import NotEqualQueryOperator

from ..query.contains import ContainsOperator
from ..query.ends_with import EndsWithOperator
from ..query.exact import ExactOperator
from ..query.starts_with import StartsWithOperator
from ..query.i_contains import IContainsOperator
from ..query.i_ends_with import IEndsWithOperator
from ..query.i_exact import IExactOperator
from ..query.i_starts_with import IStartsWithOperator


OPERATORS = {
    'exists': ExistsQueryOperator,
    'gt': GreaterThanQueryOperator,
    'gte': GreaterThanOrEqualQueryOperator,
    'lt': LesserThanQueryOperator,
    'lte': LesserThanOrEqualQueryOperator,
    'in': InQueryOperator,
    'is_null': IsNullQueryOperator,
    'ne': NotEqualQueryOperator,
    'not': NotOperator,
    'contains': ContainsOperator,
    'endswith': EndsWithOperator,
    'exact': ExactOperator,
    'startswith': StartsWithOperator,
    'icontains': IContainsOperator,
    'iendswith': IEndsWithOperator,
    'iexact': IExactOperator,
    'istartswith': IStartsWithOperator,
}


class DefaultOperator(QueryOperator):
    def to_query(self, field_name, value):
        return {
            field_name: value
        }


# from http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def transform_query(document, **query):
    mongo_query = {}

    for key, value in sorted(query.items()):
        if key == 'raw':
            update(mongo_query, value)
            continue

        if '__' not in key:
            field = document.get_fields(key)[0]
            field_name = field.db_field
            operator = DefaultOperator()
            field_value = operator.get_value(field, value)
        else:
            values = key.split('__')
            field_reference_name, operator = ".".join(values[:-1]), values[-1]
            if operator not in OPERATORS:
                field_reference_name = "%s.%s" % (
                    field_reference_name, operator)
                operator = ""

            fields = document.get_fields(field_reference_name)

            field_name = ".".join([
                hasattr(field, 'db_field') and field.db_field or field
                for field in fields
            ])
            operator = OPERATORS.get(operator, DefaultOperator)()
            field_value = operator.get_value(fields[-1], value)

        update(mongo_query, operator.to_query(field_name, field_value))

    return mongo_query


def validate_fields(document, query):
    from .fields.embedded_document_field import EmbeddedDocumentField
    from .fields.list_field import ListField

    for key, query in sorted(query.items()):
        if '__' not in key:
            fields = document.get_fields(key)
            operator = "equals"
        else:
            values = key.split('__')
            field_reference_name, operator = ".".join(values[:-1]), values[-1]
            if operator not in OPERATORS:
                field_reference_name = "%s.%s" % (
                    field_reference_name, operator)
                operator = ""

            fields = document.get_fields(field_reference_name)

        is_none = (not fields) or (not all(fields))
        is_embedded = isinstance(fields[0], (EmbeddedDocumentField,))
        is_list = isinstance(fields[0], (ListField,))

        if is_none or (not is_embedded and not is_list and operator == ''):
            raise ValueError(
                "Invalid filter '%s': Invalid operator (if this is a sub-property, "
                "then it must be used in embedded document fields)." % key)


def transform_field_list_query(document, query_field_list):
    if not query_field_list:
        return None

    fields = {}
    for key in query_field_list.keys():
        if key == '_id':
            fields[key] = query_field_list[key]
        else:
            fields_chain = document.get_fields(key)
            field_db_name = '.'.join(
                [field.db_field for field in fields_chain])
            fields[field_db_name] = query_field_list[key]

    return fields
