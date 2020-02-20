import itertools
import operator
import sys
from typing import Any
from typing import Iterable
from typing import List
from typing import Type
from typing import TYPE_CHECKING
from typing import Union

from typing_extensions import TypedDict

from ..errors import DoesNotExist
from ..errors import MultipleObjectsReturned
from ..fields.base_field import BaseField
from ..query_builder.field_list import QueryFieldList
from ..query_builder.node import Q
from ..query_builder.node import QNot
from ..query_builder.transform import validate_fields

if TYPE_CHECKING:
    from motor.core import AgnosticCollection
    from motor.core import AgnosticCursor
    from ..document import Document

DEFAULT_LIMIT = 1000


class PaginationDict(TypedDict):
    count: int
    objects: Union[None, List['Document']]
    limit: int
    offset: int
    has_next: bool
    has_previous: bool


class QuerySet(object):
    def __init__(self, klass: Type['Document'], ):
        self.__klass__ = klass
        self._filters = {}
        self._limit = None
        self._skip = None
        self._order_fields = []
        self._loaded_fields = QueryFieldList()
        self._reference_loaded_fields = {}

    def coll(self, alias: str = None) -> 'AgnosticCollection':
        """Get motor collection class"""
        return self.__klass__._get_collection(alias)

    def get_query_from_filters(self, filters):
        if not filters:
            return {}

        query = filters.to_query(self.__klass__)
        return query

    def _get_find_cursor(self, alias) -> 'AgnosticCursor':
        """Get cursor"""
        find_arguments = {}

        if self._order_fields:
            find_arguments['sort'] = self._order_fields

        if self._limit:
            find_arguments['limit'] = self._limit

        if self._skip:
            find_arguments['skip'] = self._skip

        query_filters = self.get_query_from_filters(self._filters)
        projection = self._loaded_fields.to_query(self.__klass__)

        return self.coll(alias).find(
            query_filters,
            projection=projection,
            **find_arguments
        )

    def clone(self):
        copy_props = (
            '_filters',
            '_limit',
            '_skip',
            '_order_fields',
            '_loaded_fields',
            '_reference_loaded_fields',
        )
        qs_clone = self.new()
        for prop in copy_props:
            v = getattr(self, prop)
            setattr(qs_clone, prop, v)
        return qs_clone

    def new(self):
        new_qs = self.__class__(klass=self.__klass__)
        return new_qs

    async def create(self,
                     alias: str = None,
                     **kwargs) -> Union['Document', Any]:
        """ Creates and saved a new instance of the document. """
        document = self.__klass__(**kwargs)
        await document.save(alias=alias)
        return document

    def update_field_on_save_values(self, document, creating):
        for field_name, field in self.__klass__._fields.items():
            if field.on_save is not None:
                setattr(document, field_name, field.on_save(document, creating))

    def validate_document(self, document: 'Document') -> bool:
        if not isinstance(document, self.__klass__):
            raise ValueError(
                f"This queryset for class '{self.__klass__._class_name}' can't"
                f" save an instance of type'{document._class_name}'."
            )

        document.validate()

    async def insert(self,
                     doc_or_docs: Union['Document', List['Document']],
                     alias: str = None
                     ) -> Union['Document', Any, List[Union['Document', Any]]]:
        """ Inserts all doc_or_docs passed to this method in one go. """

        docs_to_insert = []

        if not isinstance(doc_or_docs, Iterable):
            doc_or_docs = [doc_or_docs]

        for document_index, document in enumerate(doc_or_docs):
            try:
                self.validate_document(document)
            except Exception:
                err = sys.exc_info()[1]
                raise ValueError(
                    f"Validation for document {document_index} in the "
                    f"documents you are saving failed with: {str(err)}"
                )

            docs_to_insert.append(document.to_son())

        await self.coll(alias).insert_many(docs_to_insert)

    def transform_definition(self, definition) -> dict:
        result = {}
        for key, value in definition.items():
            field = self.__klass__.get_field_by_db_name(key)
            if field and field.validate(value):
                result[field.db_field] = value
            else:
                result[key] = value
        return result

    async def update(self, alias: str = None, upsert=False, **kwargs):
        """bulk update document"""
        definition = self.transform_definition(kwargs)
        update_filters = {}
        if self._filters:
            update_filters = self.get_query_from_filters(self._filters)

        update_arguments = dict(
            filter=update_filters,
            update={'$set': definition},
            upsert=upsert
        )
        return await self.coll(alias).update_many(**update_arguments)

    async def update_or_create(self, definition, alias: str = None):
        """bulk update document, if not exists then create. """
        return await self.update(**definition, alias=alias, upsert=True)

    async def delete(self, alias: str = None):
        """ Removes all instances of this document that match the specified \
        filters (if any). """

        return await self.remove(alias=alias)

    async def remove(self, instance=None, alias: str = None):
        if instance is not None:
            if hasattr(instance, 'id') and instance.id:
                return await self.coll(alias).delete_one({'_id': instance.id})
        else:
            if self._filters:
                remove_filters = self.get_query_from_filters(self._filters)
                return await self.coll(alias).delete_many(remove_filters)
            else:
                return await self.coll(alias).delete_many({})

    def only(self, *fields) -> 'QuerySet':
        """Load only a subset of this document's fields.

        :param fields: fields to include
        """

        only_fields = {}
        for field_name in fields:
            if isinstance(field_name, (BaseField,)):
                field_name = field_name.name

            only_fields[field_name] = QueryFieldList.ONLY

        return self.fields(True, **only_fields)

    def exclude(self, *fields) -> 'QuerySet':
        """Opposite to `.only()`, exclude some document's fields. """

        exclude_fields = {}
        for field_name in fields:
            if isinstance(field_name, (BaseField,)):
                field_name = field_name.name

            exclude_fields[field_name] = QueryFieldList.EXCLUDE

        return self.fields(**exclude_fields)

    def fields(self, _only_called=False, **kwargs) -> 'QuerySet':
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

        :param _only_called:
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

            cleaned_fields.append((key, value))

        # divide fields on groups by their values
        # (ONLY group, EXCLUDE group etc.) and add them to _loaded_fields
        # as an appropriate QueryFieldList
        fields = sorted(cleaned_fields, key=operator.itemgetter(1))
        for value, group in itertools.groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            self._loaded_fields += QueryFieldList(
                fields, value=value, _only_called=_only_called)

        return self

    def all_fields(self) -> 'QuerySet':
        """Include all fields.

        Reset all previously calls of `.only()` or `.exclude().`

        Usage::

            # this will load 'comments' too
            BlogPost.objects.exclude("comments").all_fields().get(...)
        """
        self._loaded_fields = QueryFieldList(
            always_include=self._loaded_fields.always_include)

        return self

    async def get(self, alias: str = None, **kwargs) -> Union['Document', Any]:
        """ Gets a single item of the current queryset collection using it's id.

        In order to query a different database, please specify the `alias` \
        of the database to query.
        """

        if not kwargs:
            raise RuntimeError(
                "Either an id or a filter must be provided to get")
        # _id = kwargs.get('id')
        # if _id is not None:
        #     if not isinstance(_id, ObjectId):
        #         _id = ObjectId(_id)
        #     filters = {"_id": _id}
        # else:
        # filters = Q(**kwargs)
        # filters = self.get_query_from_filters(filters)
        count = await self.new().filter(**kwargs).limit(2).count()
        if count > 1:
            raise MultipleObjectsReturned
        if count == 0:
            raise DoesNotExist
        obj = await self.new().filter(**kwargs).first(alias=alias)
        return obj

    def filter(self, *arguments, **kwargs) -> 'QuerySet':
        """ Filters a queryset in order to produce a different set of document \
        from subsequent queries. """

        if arguments and len(arguments) == 1 and \
                isinstance(arguments[0], (Q, QNot)):
            if self._filters:
                self._filters = self._filters & arguments[0]
            else:
                self._filters = arguments[0]
        else:
            validate_fields(self.__klass__, kwargs)
            if self._filters:
                self._filters = self._filters & Q(**kwargs)
            else:
                if arguments and len(arguments) == 1 and isinstance(
                        arguments[0], dict):
                    self._filters = Q(arguments[0])
                else:
                    self._filters = Q(**kwargs)
        return self

    def filter_not(self, *arguments, **kwargs) -> 'QuerySet':
        """ Filters a queryset to negate all the filters passed in subsequent \
        queries. """

        if arguments and len(arguments) == 1 and \
                isinstance(arguments[0], (Q, QNot)):
            self.filter(QNot(arguments[0]))
        else:
            self.filter(QNot(Q(**kwargs)))

        return self

    def skip(self, skip: int) -> 'QuerySet':
        """ Skips N documents before returning in subsequent queries. """

        self._skip = skip
        return self

    async def first(self, alias: str = None) -> Union[None, 'Document', Any]:
        """ Limits the number of documents to return in subsequent queries. """

        cursor = self._get_find_cursor(alias=alias)
        ret = await cursor.to_list(1)
        if ret:
            return self.__klass__.from_son(ret[0])
        else:
            return None

    def limit(self, limit: int) -> 'QuerySet':
        """ Limits the number of documents to return in subsequent queries. """

        self._limit = limit
        return self

    def order_by(self, *fields: str) -> 'QuerySet':
        """ Specified the order to be used when returning documents in \
        subsequent queries.
        """

        _raw_order_fields = []
        for field in fields:
            if field.startswith('-'):
                _raw_order_fields.append((field[1:], -1))
            else:
                _raw_order_fields.append((field, 1))

        self._order_fields.extend(_raw_order_fields)
        return self

    async def find_all(self, alias: str = None) -> Union[list, None]:
        """ Returns a list of items in the current queryset collection that \
        match specified filters (if any). """

        length = self._limit or DEFAULT_LIMIT
        cursor = self._get_find_cursor(alias=alias)
        return await cursor.to_list(length)

    async def all(self, alias: str = None):
        return await self.find_all(alias=alias)

    async def count(self, alias: str = None) -> int:
        """ Returns the number of documents in the collection that match the \
        specified filters, if any. """
        query_filters = self.get_query_from_filters(self._filters)
        return await self.coll(alias).count_documents(query_filters)

    async def exists(self, alias: str = None):
        return bool(await self.count(alias=alias))

    async def pagination(self,
                         limit: int = 10,
                         offset: int = 0,
                         alias: str = None) -> PaginationDict:

        if not isinstance(limit, int):
            limit = int(limit)
        if not isinstance(offset, int):
            offset = int(offset)

        count = await self.count()
        has_next = count > (limit + offset)
        has_previous = offset > 0
        objects = await self.skip(offset).limit(limit).all(alias=alias)
        return PaginationDict(
            count=count,
            objects=objects,
            limit=limit,
            offset=offset,
            has_next=has_next,
            has_previous=has_previous
        )
