__version__ = "0.0.1.dev"

try:
    from pymongo import ASCENDING, DESCENDING  # NOQA

    from .connection import connect, disconnect  # NOQA
    from .document import Document, get_collections, get_collection_list  # NOQA

    from .fields import (  # NOQA
        BaseField, StringField, BooleanField, DateTimeField,
        UUIDField, ListField, EmbeddedDocumentField, ReferenceField, URLField,
        EmailField, IntField, FloatField, DecimalField, BinaryField,
        JsonField, ObjectIdField
    )

    # from .aggregation.base import Aggregation  # NOQA
    from .query_builder.node import Q, QNot  # NOQA

except ImportError as e:  # NOQA
    # likely setup.py trying to import version
    import sys
    import traceback
    traceback.print_exception(*sys.exc_info())
