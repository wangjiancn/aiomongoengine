from copy import deepcopy
from typing import List
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

from pymongo import ASCENDING
from pymongo import DESCENDING
from pymongo import HASHED
from pymongo import IndexModel
from pymongo import TEXT

if TYPE_CHECKING:
    from .document import Document

INDEX_TYPE_MAP = {
    '$': TEXT,
    '#': HASHED,
    '-': DESCENDING
}


def camel_lowercase(camel_string):
    """ Convert camel string to lowercase. """
    lowercase_string = camel_string[0].lower()
    for s in camel_string[1:]:
        if s.isupper():
            lowercase_string += f'_{s.lower()}'
        else:
            lowercase_string += s
    return lowercase_string


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


_class_registry_cache = {}
_field_list_cache = []


def _import_class(cls_name):
    """Cache mechanism for imports.

    Due to complications of circular imports aiomongoengine needs to do lots of
    inline imports in functions.  This is inefficient as classes are
    imported repeated throughout the aiomongoengine code.  This is
    compounded by some recursive functions requiring inline imports.

    :mod:`aiomongoengine.common` provides a single point to import all these
    classes.  Circular imports aren't an issue as it dynamically imports the
    class when first needed.  Subsequent calls to the
    :func:`~aiomongoengine.common._import_class` can then directly retrieve the
    class from the :data:`aiomongoengine.common._class_registry_cache`.
    """
    if cls_name in _class_registry_cache:
        return _class_registry_cache.get(cls_name)

    doc_classes = ("Document", "BaseDocument")

    # Field Classes
    if not _field_list_cache:
        from aiomongoengine.fields import __all__ as fields
        _field_list_cache.extend(fields)

    field_classes = _field_list_cache

    if cls_name in doc_classes:
        from aiomongoengine import document as module
        import_classes = doc_classes
    elif cls_name in field_classes:
        from aiomongoengine import fields as module
        import_classes = field_classes
    else:
        raise ValueError("No import set for: %s" % cls_name)
    for cls in import_classes:
        _class_registry_cache[cls] = getattr(module, cls)

    return _class_registry_cache.get(cls_name)
