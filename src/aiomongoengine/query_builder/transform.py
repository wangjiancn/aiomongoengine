import collections

from ..query.base import QueryOperator
from ..query.base import QUERY_OPERATORS


class DefaultOperator(QueryOperator):
    def to_query(self, field_name, value):
        return {field_name: value}


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def transform_query(document=None, **query):
    mongo_query = {}

    for key, value in sorted(query.items()):
        if key == 'raw':
            update(mongo_query, value)
            continue

        if '__' not in key:
            if document:
                field = document.get_fields(key)[0]
                field_name = field.db_field
            else:
                field = None
                field_name = key
            operator = DefaultOperator()
            field_value = operator.get_value(field, value)
        else:
            values = key.split('__')
            field_reference_name, operator = ".".join(values[:-1]), values[-1]
            if operator not in QUERY_OPERATORS:
                field_reference_name = "%s.%s" % (
                    field_reference_name, operator)
                operator = ""

            fields = document.get_fields(field_reference_name)

            field_name = ".".join([
                hasattr(field, 'db_field') and field.db_field or field
                for field in fields
            ])
            operator = QUERY_OPERATORS.get(operator, DefaultOperator)()
            field_value = operator.get_value(fields[-1], value)

        update(mongo_query, operator.to_query(field_name, field_value))

    return mongo_query


def validate_fields(document, query):
    from aiomongoengine.fields.embedded_document_field import \
        EmbeddedDocumentField
    from aiomongoengine.fields.list_field import ListField

    for key, query in sorted(query.items()):
        if '__' not in key:
            fields = document.get_fields(key)
            operator = "equals"
        else:
            values = key.split('__')
            field_reference_name, operator = ".".join(values[:-1]), values[-1]
            if operator not in QUERY_OPERATORS:
                field_reference_name = "%s.%s" % (
                    field_reference_name, operator)
                operator = ""

            fields = document.get_fields(field_reference_name)

        is_none = (not fields) or (not all(fields))
        is_embedded = isinstance(fields[0], (EmbeddedDocumentField,))
        is_list = isinstance(fields[0], (ListField,))

        if is_none or (not is_embedded and not is_list and operator == ''):
            raise ValueError(
                f"Invalid filter '{key}': Invalid operator (if this is a "
                "sub-property, then it must be used in embedded document "
                "fields).")


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
