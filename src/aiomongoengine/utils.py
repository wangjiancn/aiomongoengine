from copy import deepcopy
from typing import List
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

import sys
from pymongo import ASCENDING
from pymongo import DESCENDING
from pymongo import HASHED
from pymongo import IndexModel
from pymongo import TEXT

try:
    from ujson import loads, dumps


    def serialize(value):
        return dumps(value)


    def deserialize(value):
        return loads(value)

except ImportError:
    from json import loads, dumps
    from bson import json_util


    def serialize(value):
        return dumps(value, default=json_util.default)


    def deserialize(value):
        return loads(value, object_hook=json_util.object_hook)

if TYPE_CHECKING:
    from .document import Document


def get_class(module_name, klass=None):
    if '.' not in module_name and klass is None:
        raise ImportError("Can't find class %s." % module_name)

    try:
        module_parts = module_name.split('.')

        if klass is None:
            module_name = '.'.join(module_parts[:-1])
            klass_name = module_parts[-1]
        else:
            klass_name = klass

        module = __import__(module_name)

        if '.' in module_name:
            for part in module_name.split('.')[1:]:
                module = getattr(module, part)

        return getattr(module, klass_name)
    except AttributeError:
        err = sys.exc_info()
        raise ImportError("Can't find class %s (%s)." %
                          (module_name, str(err)))


"""
[
            'title',
            '$title',  # text index
            '#title',  # hashed index
            ('title', '-rating'),
            ('category', '_cls'),
            {
                'fields': ['created'],
                'expireAfterSeconds': 3600
            }
        ]
"""

INDEX_TYPE_MAP = {
    '$': TEXT,
    '#': HASHED,
    '-': DESCENDING
}


def parse_single_index(index_str) -> Tuple[str, Union[str, int]]:
    index_type = INDEX_TYPE_MAP.get(index_str[0])
    if index_type:
        return index_str[1:], index_type
    else:
        return index_str, ASCENDING


def parse_indexes(indexes: list,
                  collection: 'Document' = None) -> List[IndexModel]:
    index_model_list = []
    for index in indexes:
        if isinstance(index, str):
            index_model_list.append(IndexModel([parse_single_index(index)]))
        elif isinstance(index, tuple):
            index_model_list.append(
                IndexModel([parse_single_index(i) for i in index]))
        elif isinstance(index, dict):
            if isinstance(index.get('fields'), list):
                index_clone = deepcopy(index)
                index_fields = [parse_single_index(i) for i in
                                index_clone.pop('fields', [])]
                index_model_list.append(
                    IndexModel(index_fields, **index_clone)
                )
    return index_model_list
