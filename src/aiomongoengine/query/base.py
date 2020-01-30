from ..fields.base_field import BaseField
from functools import wraps

QUERY_OPERATORS = {}
UPDATE_OPERATORS = {}


class QueryOperator(object):
    op = None

    def get_value(self, field, raw_value):
        if field is None or not isinstance(field, (BaseField,)):
            return raw_value

        return field.to_query(raw_value)

    def to_query(self, *args, **kwargs):
        raise NotImplementedError()


class UpdateOperator(object):
    op = None

    def to_update(self, *args, **kwargs):
        raise NotImplementedError()


def add_update_operator(cls: QueryOperator):
    op = cls.op
    if cls.op is not None and not UPDATE_OPERATORS.get(cls.op):
        UPDATE_OPERATORS[op] = cls
    return cls


def add_query_operator(cls: QueryOperator):
    op = cls.op
    if cls.op is not None and not QUERY_OPERATORS.get(cls.op):
        QUERY_OPERATORS[op] = cls
    return cls
