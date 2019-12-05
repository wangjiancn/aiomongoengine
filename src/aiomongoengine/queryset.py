from __future__ import annotations
from typing import TYPE_CHECKING
import sys
import operator
import itertools

from motor.motor_asyncio import (
    AsyncIOMotorCollection,
    AsyncIOMotorCursor
)
from pymongo.errors import DuplicateKeyError
from tornado.concurrent import return_future
from easydict import EasyDict as edict
from bson.objectid import ObjectId

from pymongo import ASCENDING
from .aggregation.base import Aggregation
from .connection import get_connection
from .errors import (
    UniqueKeyViolationError,
    PartlyLoadedDocumentError,
    DoesNotExist,
    MultipleObjectsReturned
)
from .query_builder.field_list import QueryFieldList

if TYPE_CHECKING:
    from aiomongoengine import Document

DEFAULT_LIMIT = 1000

# Motor doc
# https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_collection.html
# https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_database.html
# https://motor.readthedocs.io/en/stable/api-tornado/cursors.html


class QuerySet(object):
    def __init__(self, klass: Document) -> QuerySet:
        self.__klass__ = klass  # Document 类
        self._filters = {}  # 查询条件
        self._limit = None
        self._skip = None
        self._order_fields = []  # 排序字段
        self._loaded_fields = QueryFieldList()
        self._reference_loaded_fields = {}

    @property
    def is_lazy(self):
        return self.__klass__.__lazy__

    def coll(self, alias: str = None) -> AsyncIOMotorCollection:
        """Get motor collection class"""
        if alias is not None:
            conn = get_connection(alias=alias)
        elif self.__klass__.__alias__ is not None:
            conn = get_connection(alias=self.__klass__.__alias__)
        else:
            conn = get_connection()

        return conn[self.__klass__.__collection__]

    def get_query_from_filters(self, filters):
        if not filters:
            return {}

        query = filters.to_query(self.__klass__)
        return query

    def _get_find_cursor(self, alias):
        """Get cursor"""
        find_arguments = {}

        if self._order_fields:
            find_arguments['sort'] = self._order_fields

        if self._limit:
            find_arguments['limit'] = self._limit

        if self._skip:
            find_arguments['skip'] = self._skip

        query_filters = self.get_query_from_filters(self._filters)

        return self.coll(alias).find(
            query_filters,
            projection=self._loaded_fields.to_query(self.__klass__),
            **find_arguments
        )

    async def create(self, alias=None, **kwargs):
        ''' Creates and saved a new instance of the document. '''
        document = self.__klass__(**kwargs)
        await self.save(document=document, alias=alias)
        return document

    def update_field_on_save_values(self, document, creating):
        for field_name, field in self.__klass__._fields.items():
            if field.on_save is not None:
                setattr(document, field_name,
                        field.on_save(document, creating))

    async def save(self, document, alias=None, upsert=False):
        """save a document"""
        if document.is_partly_loaded:
            msg = (
                "Partly loaded document {0} can't be saved. Document should "
                "be loaded without 'only', 'exclude' or 'fields' "
                "QuerySet's modifiers"
            )
            raise PartlyLoadedDocumentError(
                msg.format(document.__class__.__name__)
            )

        if self.validate_document(document):
            doc = document.to_son()
            _id = doc.pop('_id', None)
            if _id is not None:
                await self.coll(alias).find_one_and_update(
                    {'_id': document._id},
                    {'$set': doc},
                    upsert=upsert
                )
            else:
                ret = await self.coll(alias).insert_one(doc)
                document._id = ret.inserted_id
            document._values = doc
            return document

    def validate_document(self, document):
        if not isinstance(document, self.__klass__):
            raise ValueError("This queryset for class '%s' can't save an instance of type '%s'." % (
                self.__klass__.__name__,
                document.__class__.__name__,
            ))

        return document.validate()

    async def bulk_insert(self, documents, callback=None, alias=None):
        ''' Inserts all documents passed to this method in one go. '''

        is_valid = True
        docs_to_insert = []

        for document_index, document in enumerate(documents):
            try:
                is_valid = is_valid and self.validate_document(document)
            except Exception:
                err = sys.exc_info()[1]
                raise ValueError("Validation for document %d in the documents you are saving failed with: %s" % (
                    document_index,
                    str(err)
                ))

            if not is_valid:
                return

            docs_to_insert.append(document.to_son())

        if not is_valid:
            return

        await self.coll(alias).insert_many(docs_to_insert)

    def transform_definition(self, definition):
        from .fields.base_field import BaseField

        result = {}

        for key, value in definition.items():
            if isinstance(key, (BaseField, )):
                result[key.db_field] = value
            else:
                result[key] = value

        return result

    async def update(self, definition, alias=None, upsert=False):
        """bulk update document"""
        # TODO valide field

        definition = self.transform_definition(definition)

        update_filters = {}
        if self._filters:
            update_filters = self.get_query_from_filters(self._filters)

        update_arguments = dict(
            filter=update_filters,
            update={'$set': definition},
            upsert=upsert
        )
        return await self.coll(alias).update_many(**update_arguments)

    async def update_or_create(self, definition, alias=None):
        """bulk update document, if not exists then create. """
        return await self.update(definition, alias=alias, upsert=True)

    async def delete(self, callback=None, alias=None):
        ''' Removes all instances of this document that match the specified filters (if any). '''

        return await self.remove(alias=alias)

    async def remove(self, instance=None, alias=None):
        if instance is not None:
            if hasattr(instance, '_id') and instance._id:
                await self.coll(alias).delete_one({'_id': instance._id})
        else:
            if self._filters:
                remove_filters = self.get_query_from_filters(self._filters)
                await self.coll(alias).delete_many(remove_filters)
            else:
                await self.coll(alias).delete_many({})

    def _check_valid_field_name_to_project(self, field_name, value):
        """Determine a presence of the field_name in the document.

        Helper method that determines a presence of the field_name in document
        including embedded documents' fields, lists of embedded documents,
        reference fields and lists of reference fields.

        :param field_name: name of the field, ex.: `title`, `author.name`
        :param value: projection value such as ONLY, EXCLUDE or slice dict
        :returns: tuple of field name and projection value
        """
        if '.' not in field_name and (
            field_name == '_id' or field_name in self.__klass__._fields
        ):
            return (field_name, value)

        from .fields.embedded_document_field import (
            EmbeddedDocumentField
        )
        from .fields.list_field import ListField
        from .fields.reference_field import ReferenceField
        from .document import BaseDocument

        tail = field_name
        head = []  # part of the name before reference
        obj = self.__klass__
        while tail:
            parts = tail.split('.', 1)
            if len(parts) == 2:
                field_value, tail = parts
            else:
                field_value, tail = parts[0], None
            head.append(field_value)

            if not obj or field_value not in obj._fields:
                raise ValueError(
                    "Invalid field '%s': Field not found in '%s'." % (
                        field_name, self.__klass__.__name__
                    )
                )
            else:
                field = obj._fields[field_value]
                if isinstance(field, EmbeddedDocumentField):
                    obj = field.embedded_type
                elif isinstance(field, ListField):
                    if hasattr(field._base_field, 'embedded_type'):
                        # list of embedded documents
                        obj = field.item_type
                    elif hasattr(field._base_field, 'reference_type'):
                        # list of reference fields
                        return self._fill_reference_loaded_fields(
                            head, tail, field_name, value
                        )
                    else:
                        obj = None
                elif (isinstance(field, ReferenceField)):
                    return self._fill_reference_loaded_fields(
                        head, tail, field_name, value
                    )
                else:
                    obj = None

        return (field_name, value)

    def _fill_reference_loaded_fields(self, head, tail, field_name, value):
        """Helper method to process reference field in projection.

        :param head: list of parts of the field_name before reference
        :param tail: reference document's part of the name
        :param field_name: full field name
        :param value: ONLY, EXCLUDE or slice dict

        :returns: tuple of field name (or its not a reference part) and
        projection value
        """
        name = '.'.join(head)
        if tail:
            # there is some fields for referenced document
            if name not in self._reference_loaded_fields:
                self._reference_loaded_fields[name] = {}
            self._reference_loaded_fields[name][tail] = value
            # and we should include reference field explicitly
            return (name, QueryFieldList.ONLY)
        else:
            return (field_name, value)

    def only(self, *fields):
        """Load only a subset of this document's fields.

        Usage::

            BlogPost.objects.only(BlogPost.title, "author.name").find_all(...)

        .. note ::

            `only()` is chainable and will perform a union. So with the
            following it will fetch both: `title` and `comments`::

                BlogPost.objects.only("title").only("comments").get(...)

        .. note :: `only()` does not exclude `_id` field

        :func:`~.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to include

        """
        from .fields.base_field import BaseField

        only_fields = {}
        for field_name in fields:
            if isinstance(field_name, (BaseField, )):
                field_name = field_name.name

            only_fields[field_name] = QueryFieldList.ONLY

        # self.only_fields = fields.keys()
        return self.fields(True, **only_fields)

    def exclude(self, *fields):
        """Opposite to `.only()`, exclude some document's fields.

        Usage::

            BlogPost.objects.exclude("_id", "comments").get(...)

        .. note ::

            `exclude()` is chainable and will perform a union. So with the
            following it will exclude both: `title` and `author.name`::

                BlogPost.objects.exclude(BlogPost.title).exclude("author.name").get(...)

        .. note ::

            if `only()` is called somewhere in chain then `exclude()` calls
            remove fields from the lists of fields specified by `only()` calls::

                # this will load all fields
                BlogPost.objects.only('title').exclude('title').find_all(...)

                # this will load only 'title' field
                BlogPost.objects.only('title').exclude('comments').get(...)

                # this will load only 'title' field
                BlogPost.objects.exclude('comments').only(
                    'title', 'comments').get(...)

                # there is one exception for _id field,
                # which will be excluded even if only() is called,
                # actually the following is the only way to exclude _id field
                BlogPost.objects.only('title').exclude('_id').find_all(...)

        :func:`~.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to exclude

        """
        from .fields.base_field import BaseField

        exclude_fields = {}
        for field_name in fields:
            if isinstance(field_name, (BaseField, )):
                field_name = field_name.name

            exclude_fields[field_name] = QueryFieldList.EXCLUDE

        return self.fields(**exclude_fields)

    def fields(self, _only_called=False, **kwargs):
        """Manipulate how you load this document's fields.

        Used by `.only()` and `.exclude()` to manipulate which fields to
        retrieve. Fields also allows for a greater level of control
        for example:

        Retrieving a Subrange of Array Elements:

        You can use the `$slice` operator to retrieve a subrange of elements in
        an array. For example to get the first 5 comments::

            BlogPost.objects.fields(slice__comments=5).get(...)

        or 5 comments after skipping 10 comments::

            BlogPost.objects.fields(slice__comments=(10, 5)).get(...)

        or you can also use negative values, for example skip 10 comment from
        the end and retrieve 5 comments forward::

            BlogPost.objects.fields(slice__comments=(-10, 5)).get(...)

        Besides slice, it is possible to include or exclude fields
        (but it is strongly recommended to use `.only()` and `.exclude()`
        methods instead)::

            BlogPost.objects.fields(
                slice__comments=5,
                _id=QueryFieldList.EXCLUDE,
                title=QueryFieldList.ONLY
            ).get(...)

        :param kwargs: A dictionary identifying what to include
        """

        # Check for an operator and transform to mongo-style if there is one
        operators = ["slice"]
        cleaned_fields = []
        for key, value in kwargs.items():
            parts = key.split('__')
            if parts[0] in operators:
                op = parts.pop(0)
                value = {'$' + op: value}

            key = '.'.join(parts)
            try:
                field_name, value = self._check_valid_field_name_to_project(
                    key, value
                )
            except ValueError as e:
                raise e

            cleaned_fields.append((field_name, value))

        # divide fields on groups by their values
        # (ONLY group, EXCLUDE group etc.) and add them to _loaded_fields
        # as an appropriate QueryFieldList
        fields = sorted(cleaned_fields, key=operator.itemgetter(1))
        for value, group in itertools.groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            self._loaded_fields += QueryFieldList(
                fields, value=value, _only_called=_only_called)

        return self

    def all_fields(self):
        """Include all fields.

        Reset all previously calls of `.only()` or `.exclude().`

        Usage::

            # this will load 'comments' too
            BlogPost.objects.exclude("comments").all_fields().get(...)
        """
        self._loaded_fields = QueryFieldList(
            always_include=self._loaded_fields.always_include)

        return self

    def handle_auto_load_references(self, doc, callback):
        def handle(*args, **kw):
            if len(args) > 0:
                callback(doc)
                return

            callback(None)

        return handle

    async def get(self, alias=None, **kwargs):
        '''
        Gets a single item of the current queryset collection using it's id.

        In order to query a different database, please specify the `alias` of the database to query.
        '''

        from import Q

        if not kwargs:
            raise RuntimeError(
                "Either an id or a filter must be provided to get")
        id = kwargs.get('id') or kwargs.get('_id')
        if id is not None:
            if not isinstance(id, ObjectId):
                id = ObjectId(id)
            filters = {
                "_id": id
            }
        else:
            filters = Q(**kwargs)
            filters = self.get_query_from_filters(filters)
        count = await self.coll(alias).count_documents(filters, limit=2)
        if count > 1:
            raise MultipleObjectsReturned
        if count == 0:
            raise DoesNotExist
        document = await self.coll(alias).find_one(filters, projection=self._loaded_fields.to_query(self.__klass__))
        obj = self.__klass__.from_son(document)
        return obj

    def filter(self, *arguments, **kwargs):
        '''
        Filters a queryset in order to produce a different set of document from subsequent queries.

        Usage::

            User.objects.filter(first_name="Bernardo").filter(last_name="Bernardo").find_all(callback=handle_all)
            # or
            User.objects.filter(first_name="Bernardo", starting_year__gt=2010).find_all(
                callback=handle_all)

        The available filter options are the same as used in MongoEngine.
        '''
        from .query_builder.node import Q, QCombination, QNot
        from .query_builder.transform import validate_fields

        if arguments and len(arguments) == 1 and isinstance(arguments[0], (Q, QNot, QCombination)):
            if self._filters:
                self._filters = self._filters & arguments[0]
            else:
                self._filters = arguments[0]
        else:
            validate_fields(self.__klass__, kwargs)
            if self._filters:
                self._filters = self._filters & Q(**kwargs)
            else:
                if arguments and len(arguments) == 1 and isinstance(arguments[0], dict):
                    self._filters = Q(arguments[0])
                else:
                    self._filters = Q(**kwargs)

        return self

    def filter_not(self, *arguments, **kwargs):
        '''
        Filters a queryset to negate all the filters passed in subsequent queries.

        Usage::

            User.objects.filter_not(first_name="Bernardo").filter_not(last_name="Bernardo").find_all(callback=handle_all)
            # or
            User.objects.filter_not(
                first_name="Bernardo", starting_year__gt=2010).find_all(callback=handle_all)

        The available filter options are the same as used in MongoEngine.
        '''
        from .query_builder.node import Q, QCombination, QNot

        if arguments and len(arguments) == 1 and isinstance(arguments[0], (Q, QCombination)):
            self.filter(QNot(arguments[0]))
        else:
            self.filter(QNot(Q(**kwargs)))

        return self

    def skip(self, skip):
        '''
        Skips N documents before returning in subsequent queries.

        Usage::

            User.objects.skip(20).limit(10).find_all(
                callback=handle_all)  # even if there are 100s of users,
                                                                           # only users 20-30 will be returned
        '''

        self._skip = skip
        return self

    async def first(self, alias=None):
        '''
        Limits the number of documents to return in subsequent queries.

        Usage::

            # even if there are 100s of users,
            User.objects.limit(10).find_all(callback=handle_all)
                                                                  # only first 10 will be returned
        '''

        cursor = self._get_find_cursor(alias=alias)
        ret = await cursor.to_list(1)
        if ret:
            return self.__klass__.from_son(ret[0])
        else:
            return None

    def limit(self, limit):
        '''
        Limits the number of documents to return in subsequent queries.

        Usage::

            # even if there are 100s of users,
            User.objects.limit(10).find_all(callback=handle_all)
                                                                  # only first 10 will be returned
        '''

        self._limit = limit
        return self

    def order_by(self, *fields: str) -> 'self':
        '''
        Specified the order to be used when returning documents in subsequent queries.

        Usage::
            User.objects.order_by('first','-second').find_all()
        '''

        _raw_order_fileds = []
        for field in fields:
            if field.startswith('-'):
                _raw_order_fileds.append((field, -1))
            else:
                _raw_order_fileds.append((field, 1))

        self._order_fields.extend(_raw_order_fileds)
        return self

    async def find_all(self,  alias=None) -> list:
        ''' Returns a list of items in the current queryset collection that match specified filters (if any). '''

        length = self._limit or DEFAULT_LIMIT
        cursor = self._get_find_cursor(alias=alias)
        return await cursor.to_list(length)

    async def all(self, alias=None):
        return await self.find_all(alias=alias)

    async def count(self, alias=None) -> int:
        ''' Returns the number of documents in the collection that match the specified filters, if any. '''
        query_filters = self.get_query_from_filters(self._filters)
        return await self.coll(alias).count_documents(query_filters)

    async def pagination(self, limit=10, offset=0, alias=None):
        count = await self.count()
        has_next = count > (limit + offset)
        has_previous = offset > 0
        objects = await self.skip(offset).limit(limit).all()
        return {
            'count': count,
            'objects': objects,
            'limit': limit,
            'offset': offset,
            'has_next': has_next,
            'has_previous': has_previous
        }

    @property
    def aggregate(self):
        return Aggregation(self)

    def handle_ensure_index(self, callback, created_indexes, total_indexes):
        def handle(*arguments, **kw):
            if len(arguments) > 1 and arguments[1]:
                raise arguments[1]

            created_indexes.append(arguments[0])

            if len(created_indexes) < total_indexes:
                return

            callback(total_indexes)

        return handle

    # TODO create_index
    async def ensure_index(self, callback, alias=None):
        fields_with_index = []
        for field_name, field in self.__klass__._fields.items():
            if field.unique or field.sparse:
                fields_with_index.append(field)

        created_indexes = []

        for field in fields_with_index:
            self.coll(alias).ensure_index(
                field.db_field,
                unique=field.unique,
                sparse=field.sparse
            )

        if not fields_with_index:
            callback(0)
