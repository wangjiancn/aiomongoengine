from typing import List
from typing import Union
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiomongoengine.document import Document

from ..query_builder.transform import transform_query


class QNode(object):

    def __init__(self,
                 query: Union['QNode', dict] = None,
                 op: str = '',
                 document: 'Document' = None,
                 nodes: List['QNode'] = None,
                 **kwargs):
        self.query = query or kwargs
        self.op = op
        self.document = document
        self.nodes = nodes or []

    def validate_op(self, op):
        if op not in ('$and', '$or'):
            raise ValueError(f'op must in `$and, $or`, got {self.op}')

    def is_empty(self):
        return self.query

    def _combine(self, other: 'QNode', op) -> 'QNode':
        self.validate_op(op)
        if not self.op:
            self.op = op
        self.nodes.append(other)
        return self

    def _transform_query(self, document=None):
        if isinstance(self.query, QNode) and self.query.op:
            query = self.query.to_query(document)
        else:
            query = self.query
        return transform_query(document or self.document, **query)

    def to_query(self, document=None):
        raise NotImplementedError()

    def __or__(self, other: 'QNode') -> 'QNode':
        return self._combine(other, '$or')

    def __and__(self, other: 'QNode') -> 'QNode':
        return self._combine(other, '$and')

    def __invert__(self):
        return QNot(query=self)


class Q(QNode):
    def to_query(self, document=None):
        nodes = [self._transform_query(document)]
        for node in self.nodes:
            if isinstance(node, QNode):
                nodes.append(node.to_query(document))
        if self.op:
            return {self.op: nodes}
        else:
            query_dict = {}
            for node in nodes:
                query_dict.update(node)
            return query_dict


class QNot(QNode):
    def to_query(self, document=None):
        if isinstance(self.query, QNode):
            query = self.query.to_query(document)
        else:
            query = self._transform_query(document)
        result = {}
        for key, value in query.items():
            if isinstance(value, (dict,)):
                result[key] = {
                    "$not": value
                }
            elif isinstance(value, (tuple, set, list)):
                result[key] = {
                    "$nin": value
                }
            else:
                result[key] = {
                    "$ne": value
                }

        return result
