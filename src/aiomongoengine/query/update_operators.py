from .base import UpdateOperator
from .base import add_update_operator


@add_update_operator
class AddToSetOperator(UpdateOperator):
    op = 'addToSet'

    def to_update(self, field_name, value, **kwargs):
        return {'$addToSet': {field_name: value}}


@add_update_operator
class AddToSetOperator(UpdateOperator):
    op = 'inc'

    def to_update(self, field_name, value, **kwargs):
        return {'$inc': {field_name: value}}
