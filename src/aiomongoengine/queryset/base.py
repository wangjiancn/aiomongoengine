from __future__ import absolute_import

import copy
import itertools
import re
import warnings
from builtins import DeprecationWarning
from typing import Callable
from typing import List
from typing import TYPE_CHECKING
from typing import Union
from warnings import warn

import pymongo
import pymongo.errors
import six
from aiomongoengine.base.common import get_document
from aiomongoengine.context_managers import set_write_concern
from aiomongoengine.errors import BulkWriteError
from aiomongoengine.errors import InvalidQueryError
from aiomongoengine.errors import LookUpError
from aiomongoengine.errors import NotUniqueError
from aiomongoengine.errors import OperationError
from aiomongoengine.query_builder.field_list import QueryFieldList
from aiomongoengine.query_builder.node import Q
from aiomongoengine.query_builder.node import QNode
from aiomongoengine.utils import _import_class
from aiomongoengine.utils import async_iteritems
from bson import SON
from bson import json_util
from bson.code import Code
from pymongo import WriteConcern
from pymongo.collection import ReturnDocument
from pymongo.common import validate_read_preference

from ..fields.base_field import BaseField

if TYPE_CHECKING:
    from motor.core import AgnosticCursor
    from aiomongoengine import Document

__all__ = ("BaseQuerySet", "DO_NOTHING", "NULLIFY", "CASCADE", "DENY", "PULL")

# Delete rules
DO_NOTHING = 0
NULLIFY = 1
CASCADE = 2
DENY = 3
PULL = 4


# noinspection PyUnresolvedReferences
class BaseQuerySet:
    """A set of results returned from a query. Wraps a MongoDB cursor,
    providing :class:`~mongoengine.Document` objects as the results.
    """

    __dereference = False
    _auto_dereference = True

    def __init__(self, document, collection):
        self._document = document  # type: Union[Callable[[],Document],Document]
        self._collection_obj = collection
        self._mongo_query = None
        self._query_obj = Q()
        self._cls_query = {}
        self._where_clause = None
        self._loaded_fields = QueryFieldList()
        self._ordering = None
        self._timeout = True
        self._read_preference = None
        self._iter = False
        self._scalar = []
        self._none = False
        self._as_pymongo = False
        self._search_text = None

        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get("allow_inheritance") is True:
            if len(self._document._subclasses) == 1:
                self._cls_query = {"_cls": self._document._subclasses[0]}
            else:
                self._cls_query = {"_cls": {"$in": self._document._subclasses}}
            self._loaded_fields = QueryFieldList(always_include=["_cls"])

        self._cursor_obj = None
        self._limit = None
        self._skip = None
        self._hint = -1  # Using -1 as None is a valid value for hint
        self._collation = None
        self._batch_size = None
        self.only_fields = []
        self._max_time_ms = None
        self._comment = None

    def __call__(self, q_obj=None, **query):
        """Filter the selected documents by calling the
        :class:`~mongoengine.queryset.QuerySet` with a query.

        :param q_obj: a :class:`~mongoengine.queryset.Q` object to be used in
            the query; the :class:`~mongoengine.queryset.QuerySet` is filtered
            multiple times with different :class:`~mongoengine.queryset.Q`
            objects, only the last one will be used.
        :param query: Django-style query keyword arguments.
        """
        query = Q(**query)
        if q_obj:
            # Make sure proper query object is passed.
            if not isinstance(q_obj, QNode):
                msg = (
                        "Not a query object: %s. "
                        "Did you intend to use key=value?" % q_obj
                )
                raise InvalidQueryError(msg)
            query &= q_obj

        queryset = self.clone()
        queryset._query_obj &= query
        queryset._mongo_query = None
        queryset._cursor_obj = None

        return queryset

    def __getstate__(self):
        """ Need for pickling queryset """

        obj_dict = self.__dict__.copy()

        # don't pick collection, instead pickle collection params
        obj_dict.pop("_collection_obj")

        # don't pickle cursor
        obj_dict["_cursor_obj"] = None

        return obj_dict

    def __setstate__(self, obj_dict):
        """Need for pickling queryset."""

        obj_dict["_collection_obj"] = obj_dict["_document"]._get_collection()

        # update attributes
        self.__dict__.update(obj_dict)

    def __getitem__(self, key):
        """Return a document instance corresponding to a given index if
        the key is an integer. If the key is a slice, translate its
        bounds into a skip and a limit, and return a cloned queryset
        with that skip/limit applied. For example:
        """
        queryset = self.clone()

        # Handle a slice
        if isinstance(key, slice):
            queryset._cursor_obj = queryset._cursor[key]
            queryset._skip, queryset._limit = key.start, key.stop
            if key.start and key.stop:
                queryset._limit = key.stop - key.start

            if key.step:
                msg = "'Queryset' object can not use step in slice"
                warn(msg, SyntaxWarning)
            # Allow further QuerySet modifications to be performed
            return queryset
        # Move subscriptable access to __aiter__
        else:
            raise TypeError("'Queryset' object is not subscriptable")

    async def __aiter__(self):
        raise NotImplementedError

    def __bool__(self):
        msg = "use `self.limit(1).count()` instead."
        warn(msg, DeprecationWarning)
        return True

    def _handle_result(self, raw_doc_or_docs):
        if self._as_pymongo or not raw_doc_or_docs:
            return raw_doc_or_docs
        return_one = False
        if not isinstance(raw_doc_or_docs, list):
            return_one = True
            raw_doc_or_docs = [raw_doc_or_docs]
        docs = [self._document._from_son(raw_doc, only_fields=self.only_fields)
                for raw_doc in raw_doc_or_docs]
        if not return_one:
            return docs
        else:
            return docs[0]

    # Core functions

    async def all(self) -> List[Union['Document', dict]]:
        """Returns all object or document of the current QuerySet."""
        raw_docs = await self._cursor.to_list(length=None)
        return self._handle_result(raw_docs)

    def filter(self, *q_objs, **query):
        """An alias of :meth:`~aiomongoengine.queryset.QuerySet.__call__`"""
        return self.__call__(*q_objs, **query)

    async def exists(self):
        return bool(await self.limit(1).count())

    def search_text(self, text, language=None):
        """
        Start a text search, using text indexes.
        Require: MongoDB server version 2.6+.

        :param text:
        :param language:  The language that determines the list of stop words
            for the search and the rules for the stemmer and tokenizer.
            If not specified, the search uses the default language of the index.
            For supported languages, see
            `Text Search Languages <http://docs.mongodb.org/manual/reference/text-search-languages
                /#text-search-languages>`.
        """
        queryset = self.clone()
        if queryset._search_text:
            raise OperationError("It is not possible to use search_text two times.")

        query_kwargs = SON({"$search": text})
        if language:
            query_kwargs["$language"] = language

        queryset._query_obj &= Q(__raw__={"$text": query_kwargs})
        queryset._mongo_query = None
        queryset._cursor_obj = None
        queryset._search_text = text

        return queryset

    async def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~aiomongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~aiomongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.
        """
        queryset = self.clone()
        queryset = queryset.order_by().limit(2)
        queryset = queryset.filter(*q_objs, **query)
        result = async_iteritems(queryset)
        result_count = len(result)

        if result_count == 0:
            raise queryset._document.DoesNotExist(msg)
        elif result_count == 1:
            return result[0]
        # If we were able to retrieve the 2nd doc, rewind the cursor and
        # raise the MultipleObjectsReturned exception.
        queryset.rewind()
        message = f"{result_count} items returned, instead of 1"
        raise queryset._document.MultipleObjectsReturned(message)

    async def create(self, **kwargs):
        """Create new object. Returns the saved object instance. """
        return self._document(**kwargs).save(force_insert=True)

    async def first(self):
        """Retrieve the first object matching the query."""
        queryset = self.clone()
        queryset.limit(1)
        result = async_iteritems(queryset)
        if result:
            return result[0]
        else:
            return None

    async def insert(
            self,
            doc_or_docs: Union['Document', List['Document']],
            write_concern: WriteConcern = None,
            load_bulk: bool = True
    ):

        """bulk insert documents

        :param write_concern: Extra keyword arguments are passed down to
                :meth:`~pymongo.collection.Collection.insert`
                which will be used as options for the resultant
                ``getLastError`` command.  For example,
                ``insert(..., {w: 2, fsync: True})`` will wait until at least
                two servers have recorded the write and will force an fsync on
                each server being written to.
        :param doc_or_docs: a document or list of documents to be inserted
        :param load_bulk: (optional)If True returns the list of document
            instances
        :parm signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        By default returns document instances, set ``load_bulk`` to False to
        return just ``ObjectIds``
        """

        document_cls = _import_class("Document")

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, document_cls) or issubclass(docs.__class__, document_cls):
            return_one = True
            docs = [docs]

        for doc in docs:
            if not isinstance(doc, self._document):
                msg = f"Some documents inserted aren't instances of {str(self._document)}"
                raise OperationError(msg)
            if doc.pk and not doc._created:
                msg = "Some documents have ObjectIds, use doc.update() instead"
                raise OperationError(msg)

        raw = [doc.to_mongo() for doc in docs]

        with set_write_concern(self._collection, write_concern) as collection:
            insert_func = collection.insert_many
            if return_one:
                raw = raw[0]
                insert_func = collection.insert_one

        try:
            inserted_result = await insert_func(raw)
            ids = (
                [inserted_result.inserted_id]
                if return_one
                else inserted_result.inserted_ids
            )
        except pymongo.errors.DuplicateKeyError as err:
            message = "Could not save document (%s)"
            raise NotUniqueError(message % six.text_type(err))
        except pymongo.errors.BulkWriteError as err:
            # inserting documents that already have an _id field will
            # give huge performance debt or raise
            message = u"Bulk write error: (%s)"
            raise BulkWriteError(message % six.text_type(err.details))
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", six.text_type(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = u"Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % six.text_type(err))
            raise OperationError(message % six.text_type(err))

        # Apply inserted_ids to documents
        for doc, doc_id in zip(docs, ids):
            doc.pk = doc_id

        if not load_bulk:
            return ids[0] if return_one else ids

        documents = self.in_bulk(ids)
        results = [documents.get(obj_id) for obj_id in ids]
        return results[0] if return_one else results

    async def count(self, with_limit_and_skip=False) -> int:
        """Count the selected elements in the query.

        :param with_limit_and_skip:(optional) take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count
        """
        if self._limit == 0 and with_limit_and_skip is False or self._none:
            return 0

        kwargs = {'collation': self._collation, 'hint': self._hint, 'maxTimeMS': self._max_time_ms}
        if with_limit_and_skip:
            kwargs['limit'] = self._limit
            kwargs['skip'] = self._skip

        count = self._document._get_collection().count_documents(self._query, **kwargs)
        self._cursor_obj = None
        return count

    async def delete(self, write_concern=None, _from_doc_delete=False, cascade_refs=None):
        """Delete the documents matched by the query.

        TODO: transaction support

        :param cascade_refs:
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param _from_doc_delete: True when called from document delete therefore
            signals will have been triggered so don't loop.

        :returns number of deleted documents
        """
        queryset = self.clone()
        doc = queryset._document

        if write_concern is None:
            write_concern = {}

        # # Handle deletes where skips or limits have been applied or
        # # there is an untriggered delete signal
        # has_delete_signal = signals.signals_available and (
        #         signals.pre_delete.has_receivers_for(doc)
        #         or signals.post_delete.has_receivers_for(doc)
        # )

        call_document_delete = (queryset._skip or queryset._limit) and not _from_doc_delete

        if call_document_delete:
            cnt = 0
            # TODO: use asyncio.gather
            async for doc in queryset:
                await doc.delete(**write_concern)
                cnt += 1
            return cnt

        delete_rules = doc._meta.get("delete_rules") or {}
        delete_rules = list(delete_rules.items())

        # Check for DENY rules before actually deleting/nullifying any other
        # references
        for rule_entry, rule in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get("abstract"):
                continue

            if rule == DENY:
                exists = await document_cls.objects(**{field_name + "__in": self}).exists()
                if exists:
                    raise OperationError(
                        "Could not delete document (%s.%s refers to it)"
                        % (document_cls.__name__, field_name)
                    )

        # Check all the other rules
        for rule_entry, rule in delete_rules:
            document_cls, field_name = rule_entry
            if document_cls._meta.get("abstract"):
                continue

            if rule == CASCADE:
                cascade_refs = set() if cascade_refs is None else cascade_refs
                # Handle recursive reference
                if doc._collection == document_cls._collection:
                    async for ref in queryset:
                        cascade_refs.add(ref.id)
                refs = document_cls.objects(
                    **{field_name + "__in": self, "pk__nin": cascade_refs}
                )
                if await refs.exists():
                    await refs.delete(write_concern=write_concern, cascade_refs=cascade_refs)
            elif rule == NULLIFY:
                await document_cls.objects(**{field_name + "__in": self}).update(
                    write_concern=write_concern, **{"unset__%s" % field_name: 1}
                )
            elif rule == PULL:
                await document_cls.objects(**{field_name + "__in": self}).update(
                    write_concern=write_concern, **{"pull_all__%s" % field_name: self}
                )

        with set_write_concern(queryset._collection, write_concern) as collection:
            result = await collection.delete_many(queryset._query)

            # If we're using an unack'd write _queryconcern, we don't really know how
            # many items have been deleted at this point, hence we only return
            # the count for ack'd ops.
            if result.acknowledged:
                return result.deleted_count

    async def update(
            self, upsert=False, multi=True, write_concern=None, full_result=False, **update
    ):
        """Perform an atomic update on the fields matched by the query.

        :param upsert: insert if document doesn't exist (default ``False``)
        :param multi: Update multiple documents.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param full_result: Return the associated ``pymongo.UpdateResult`` rather than just the number
            updated items
        :param update: Django-style update keyword arguments

        :returns the number of updated documents (unless ``full_result`` is True)
        """
        if not update and not upsert:
            raise OperationError("No update parameters, would remove data")

        if write_concern is None:
            write_concern = {}

        queryset = self.clone()
        query = queryset._query
        # TODO add transaction.update
        update = transform.update(queryset._document, **update)

        # If doing an atomic upsert on an inheritable class
        # then ensure we add _cls to the update operation
        if upsert and "_cls" in query:
            if "$set" in update:
                update["$set"]["_cls"] = queryset._document._class_name
            else:
                update["$set"] = {"_cls": queryset._document._class_name}
        try:
            with set_write_concern(queryset._collection, write_concern) as collection:
                update_func = collection.update_one
                if multi:
                    update_func = collection.update_many
                result = await update_func(query, update, upsert=upsert)
            if full_result:
                return result
            elif result.raw_result:
                return result.raw_result["n"]
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError(u"Update failed (%s)" % six.text_type(err))
        except pymongo.errors.OperationFailure as err:
            if six.text_type(err) == u"multi not coded yet":
                message = u"update() method requires MongoDB 1.1.3+"
                raise OperationError(message)
            raise OperationError(u"Update failed (%s)" % six.text_type(err))

    async def upsert_one(self, write_concern=None, **update):
        """Overwrite or add the first document matched by the query.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param update: Django-style update keyword arguments

        :returns the new or overwritten document
        """

        atomic_update = await self.update(
            multi=False,
            upsert=True,
            write_concern=write_concern,
            full_result=True,
            **update
        )

        if atomic_update.raw_result["updatedExisting"]:
            document = await self.get()
        else:
            document = await self._document.objects.with_id(atomic_update.upserted_id)
        return document

    async def update_one(self, upsert=False, write_concern=None, full_result=False, **update):
        """Perform an atomic update on the fields of the first document
        matched by the query.

        :param upsert: insert if document doesn't exist (default ``False``)
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param full_result: Return the associated ``pymongo.UpdateResult`` rather than just the number
            updated items
        :param update: Django-style update keyword arguments
            full_result
        :returns the number of updated documents (unless ``full_result`` is True)
        """
        return await self.update(
            upsert=upsert,
            multi=False,
            write_concern=write_concern,
            full_result=full_result,
            **update
        )

    async def modify(
            self, upsert=False, full_response=False, remove=False, new=False, **update
    ):
        """Update and return the updated document.

        Returns either the document before or after modification based on `new`
        parameter. If no documents match the query and `upsert` is false,
        returns ``None``. If upserting and `new` is false, returns ``None``.

        If the full_response parameter is ``True``, the return value will be
        the entire response object from the server, including the 'ok' and
        'lastErrorObject' fields, rather than just the modified document.
        This is useful mainly because the 'lastErrorObject' document holds
        information about the command's execution.

        :param upsert: insert if document doesn't exist (default ``False``)
        :param full_response: return the entire response object from the
            server (default ``False``, not available for PyMongo 3+)
        :param remove: remove rather than updating (default ``False``)
        :param new: return updated rather than original document
            (default ``False``)
        :param update: Django-style update keyword arguments
        """

        if remove and new:
            raise OperationError("Conflicting parameters: remove and new")

        if not update and not upsert and not remove:
            raise OperationError("No update parameters, must either update or remove")

        queryset = self.clone()
        query = queryset._query
        if not remove:
            update = transform.update(queryset._document, **update)
        sort = queryset._ordering

        try:
            if full_response:
                msg = "With PyMongo 3+, it is not possible anymore to get the full response."
                warnings.warn(msg, DeprecationWarning)
            if remove:
                result = await queryset._collection.find_one_and_delete(
                    query, sort=sort, **self._cursor_args
                )
            else:
                if new:
                    return_doc = ReturnDocument.AFTER
                else:
                    return_doc = ReturnDocument.BEFORE
                result = await queryset._collection.find_one_and_update(
                    query,
                    update,
                    upsert=upsert,
                    sort=sort,
                    return_document=return_doc,
                    **self._cursor_args
                )
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError(u"Update failed (%s)" % err)
        except pymongo.errors.OperationFailure as err:
            raise OperationError(u"Update failed (%s)" % err)

        if full_response:
            if result["value"] is not None:
                result["value"] = self._document._from_son(
                    result["value"], only_fields=self.only_fields
                )
        else:
            if result is not None:
                result = self._document._from_son(result, only_fields=self.only_fields)

        return result

    async def with_id(self, object_id):
        """Retrieve the object matching the id provided.  Uses `object_id` only
        and raises InvalidQueryError if a filter has been applied. Returns
        `None` if no document exists with that id.

        :param object_id: the value for the id of the document to look up
        """
        queryset = self.clone()
        if not queryset._query_obj.empty:
            msg = "Cannot use a filter whilst using `with_id`"
            raise InvalidQueryError(msg)
        return await queryset.filter(pk=object_id).first()

    async def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ``ObjectId``
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.
        """
        doc_map = {}

        docs = await self._collection.find({"_id": {"$in": object_ids}}, **self._cursor_args)
        if self._scalar:
            for doc in docs:
                doc_map[doc["_id"]] = self._get_scalar(
                    self._document._from_son(doc, only_fields=self.only_fields)
                )
        elif self._as_pymongo:
            for doc in docs:
                doc_map[doc["_id"]] = doc
        else:
            for doc in docs:
                doc_map[doc["_id"]] = self._document._from_son(doc, only_fields=self.only_fields)
        return doc_map

    def none(self):
        """Helper that just returns a list"""
        queryset = self.clone()
        queryset._none = True
        return queryset

    def no_sub_classes(self):
        """Filter for only the instances of this specific document.

        Do NOT return any inherited documents.
        """
        if self._document._meta.get("allow_inheritance") is True:
            self._cls_query = {"_cls": self._document._class_name}

        return self

    def using(self, alias):
        """This method is for controlling which database the QuerySet will be
        evaluated against if you are using more than one database.

        :param alias: The database alias
        """

        with switch_db(self._document, alias) as cls:
            collection = cls._get_collection()

        return self._clone_into(self.__class__(self._document, collection))

    def clone(self):
        """Create a copy of the current queryset."""
        return self._clone_into(self.__class__(self._document, self._collection_obj))

    def _clone_into(self, new_qs):
        """Copy all of the relevant properties of this queryset to
        a new queryset (which has to be an instance of
        :class:`~mongoengine.queryset.base.BaseQuerySet`).
        """
        if not isinstance(new_qs, BaseQuerySet):
            raise OperationError(
                "%s is not a subclass of BaseQuerySet" % new_qs.__name__
            )

        copy_props = (
            "_mongo_query",
            "_cls_query",
            "_none",
            "_query_obj",
            "_where_clause",
            "_loaded_fields",
            "_ordering",
            "_snapshot",
            "_timeout",
            "_slave_okay",
            "_read_preference",
            "_iter",
            "_scalar",
            "_as_pymongo",
            "_limit",
            "_skip",
            "_hint",
            "_collation",
            "_search_text",
            "only_fields",
            "_max_time_ms",
            "_comment",
            "_batch_size",
        )

        for prop in copy_props:
            val = getattr(self, prop)
            setattr(new_qs, prop, copy.copy(val))

        if self._cursor_obj:
            new_qs._cursor_obj = self._cursor_obj.clone()

        return new_qs

    # TODO refactor like django
    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects or
        :class:`~bson.object_id.ObjectId` a maximum depth in order to cut down
        the number queries to mongodb.
        """
        # Make select related work the same for querysets
        max_depth += 1
        queryset = self.clone()
        return queryset._dereference(queryset, max_depth=max_depth)

    def limit(self, n):
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).

        :param n: the maximum number of objects to return if n is greater than 0.
        When 0 is passed, returns all the documents in the cursor
        """
        queryset = self.clone()
        queryset._limit = n

        # If a cursor object has already been created, apply the limit to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.limit(queryset._limit)

        return queryset

    def skip(self, n):
        """Skip `n` documents before returning the results. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[5:]``).

        :param n: the number of objects to skip before returning results
        """
        queryset = self.clone()
        queryset._skip = n

        # If a cursor object has already been created, apply the skip to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.skip(queryset._skip)

        return queryset

    def hint(self, index=None):
        """Added 'hint' support, telling Mongo the proper index to use for the
        query.

        Judicious use of hints can greatly improve query performance. When
        doing a query on multiple fields (at least one of which is indexed)
        pass the indexed field as a hint to the query.

        Hinting will not do anything if the corresponding index does not exist.
        The last hint applied to this cursor takes precedence over all others.
        """
        queryset = self.clone()
        queryset._hint = index

        # If a cursor object has already been created, apply the hint to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.hint(queryset._hint)

        return queryset

    def collation(self, collation=None):
        """
        Collation allows users to specify language-specific rules for string
        comparison, such as rules for lettercase and accent marks.
        :param collation: `~pymongo.collation.Collation` or dict with
        following fields:
            {
                locale: str,
                caseLevel: bool,
                caseFirst: str,
                strength: int,
                numericOrdering: bool,
                alternate: str,
                maxVariable: str,
                backwards: str
            }
        Collation should be added to indexes like in test example
        """
        queryset = self.clone()
        queryset._collation = collation

        if queryset._cursor_obj:
            queryset._cursor_obj.collation(collation)

        return queryset

    def batch_size(self, size):
        """Limit the number of documents returned in a single batch (each
        batch requires a round trip to the server).

        See http://api.mongodb.com/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.batch_size
        for details.

        :param size: desired size of each batch.
        """
        queryset = self.clone()
        queryset._batch_size = size

        # If a cursor object has already been created, apply the batch size to it.
        if queryset._cursor_obj:
            queryset._cursor_obj.batch_size(queryset._batch_size)

        return queryset

    # TODO return correct value.
    async def distinct(self, field):
        """Return a list of distinct values for a given field.

        :param field: the field to select distinct values from

        .. note:: This is a command and won't take ordering or limit into
           account.
        """
        queryset = self.clone()

        try:
            field = self._fields_to_dbfields([field]).pop()
        except LookUpError:
            pass

        ret = await queryset._cursor.distinct(field)
        return ret

        # doc_field = self._document._fields.get(field.split(".", 1)[0])
        # instance = None
        #
        # # We may need to cast to the correct type eg. ListField(EmbeddedDocumentField)
        # EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        # ListField = _import_class("ListField")
        # GenericEmbeddedDocumentField = _import_class("GenericEmbeddedDocumentField")
        # if isinstance(doc_field, ListField):
        #     doc_field = getattr(doc_field, "field", doc_field)
        # if isinstance(doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)):
        #     instance = getattr(doc_field, "document_type", None)
        #
        # # handle distinct on subdocuments
        # if "." in field:
        #     for field_part in field.split(".")[1:]:
        #         # if looping on embedded document, get the document type instance
        #         if instance and isinstance(
        #                 doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)
        #         ):
        #             doc_field = instance
        #         # now get the subdocument
        #         doc_field = getattr(doc_field, field_part, doc_field)
        #         # We may need to cast to the correct type eg. ListField(EmbeddedDocumentField)
        #         if isinstance(doc_field, ListField):
        #             doc_field = getattr(doc_field, "field", doc_field)
        #         if isinstance(
        #                 doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)
        #         ):
        #             instance = getattr(doc_field, "document_type", None)
        #
        # if instance and isinstance(
        #         doc_field, (EmbeddedDocumentField, GenericEmbeddedDocumentField)
        # ):
        #     distinct = [instance(**doc) for doc in distinct]
        #
        # return distinct

    def only(self, *fields):
        """Load only a subset of this document's fields. ::

            post = BlogPost.objects(...).only('title', 'author.name')

        .. note :: `only()` is chainable and will perform a union ::
            So with the following it will fetch both: `title` and `author.name`::

                post = BlogPost.objects.only('title').only('author.name')

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to include
        """

        fields = {f: QueryFieldList.ONLY for f in fields}
        self.only_fields = fields.keys()
        return self.fields(True, **fields)

    def exclude(self, *fields):
        """Opposite to .only(), exclude some document's fields. ::

            post = BlogPost.objects(...).exclude('comments')

        .. note :: `exclude()` is chainable and will perform a union ::
            So with the following it will exclude both: `title` and `author.name`::

                post = BlogPost.objects.exclude('title').exclude('author.name')

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to exclude
        """
        fields = {f: QueryFieldList.EXCLUDE for f in fields}
        return self.fields(**fields)

    def fields(self, _only_called=False, **kwargs):
        """Manipulate how you load this document's fields. Used by `.only()`
        and `.exclude()` to manipulate which fields to retrieve. If called
        directly, use a set of kwargs similar to the MongoDB projection
        document. For example:

        Include only a subset of fields:

            posts = BlogPost.objects(...).fields(author=1, title=1)

        Exclude a specific field:

            posts = BlogPost.objects(...).fields(comments=0)

        To retrieve a subrange of array elements:

            posts = BlogPost.objects(...).fields(slice__comments=5)

        :param _only_called:
        :param kwargs: A set of keyword arguments identifying what to
            include, exclude, or slice.
        """

        # Check for an operator and transform to mongo-style if there is
        operators = ["slice"]
        cleaned_fields = []
        for key, value in kwargs.items():
            parts = key.split("__")
            if parts[0] in operators:
                op = parts.pop(0)
                value = {"$" + op: value}
            key = ".".join(parts)
            cleaned_fields.append((key, value))

        # Sort fields by their values, explicitly excluded fields first, then
        # explicitly included, and then more complicated operators such as
        # $slice.
        def _sort_key(field_tuple):
            _, value = field_tuple
            if isinstance(value, int):
                return value  # 0 for exclusion, 1 for inclusion
            return 2  # so that complex values appear last

        fields = sorted(cleaned_fields, key=_sort_key)

        # Clone the queryset, group all fields by their value, convert
        # each of them to db_fields, and set the queryset's _loaded_fields
        queryset = self.clone()
        for value, group in itertools.groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            fields = queryset._fields_to_dbfields(fields)
            queryset._loaded_fields += QueryFieldList(
                fields, value=value, _only_called=_only_called
            )

        return queryset

    def all_fields(self):
        """Include all fields. Reset all previously calls of .only() or
        .exclude(). ::

            post = BlogPost.objects.exclude('comments').all_fields()
        """
        queryset = self.clone()
        queryset._loaded_fields = QueryFieldList(
            always_include=queryset._loaded_fields.always_include
        )
        return queryset

    def order_by(self, *keys):
        """Order the :class:`~mongoengine.queryset.QuerySet` by the given keys.

        The order may be specified by prepending each of the keys by a "+" or
        a "-". Ascending order is assumed if there's no prefix.

        If no keys are passed, existing ordering is cleared instead.

        :param keys: fields to order the query results by; keys may be
            prefixed with "+" or a "-" to determine the ordering direction.
        """
        queryset = self.clone()

        old_ordering = queryset._ordering
        new_ordering = queryset._get_order_by(keys)

        if queryset._cursor_obj:

            # If a cursor object has already been created, apply the sort to it
            if new_ordering:
                queryset._cursor_obj.sort(new_ordering)

            # If we're trying to clear a previous explicit ordering, we need
            # to clear the cursor entirely (because PyMongo doesn't allow
            # clearing an existing sort on a cursor).
            elif old_ordering:
                queryset._cursor_obj = None

        queryset._ordering = new_ordering

        return queryset

    def clear_cls_query(self):
        """Clear the default "_cls" query.

        By default, all queries generated for documents that allow inheritance
        include an extra "_cls" clause. In most cases this is desirable, but
        sometimes you might achieve better performance if you clear that
        default query.

        Scan the code for `_cls_query` to get more details.
        """
        queryset = self.clone()
        queryset._cls_query = {}
        return queryset

    def comment(self, text):
        """Add a comment to the query.

        See https://docs.mongodb.com/manual/reference/method/cursor.comment/#cursor.comment
        for details.
        """
        return self._chainable_method("comment", text)

    async def explain(self):
        """Return an explain plan record for the
        :class:`~aiomongoengine.queryset.QuerySet`'s cursor.
        """
        return await self._cursor.explain()

    def timeout(self, enabled: bool):
        """Enable or disable the default mongod timeout when querying. (no_cursor_timeout option)

        :param enabled: whether or not the timeout is used
        """
        queryset = self.clone()
        queryset._timeout = enabled
        return queryset

    def read_preference(self, read_preference):
        """Change the read_preference when querying.

        :param read_preference: override ReplicaSetConnection-level
            preference.
        """
        validate_read_preference("read_preference", read_preference)
        queryset = self.clone()
        queryset._read_preference = read_preference
        # we need to re-create the cursor object whenever we apply read_preference
        queryset._cursor_obj = None
        return queryset

    def scalar(self, *fields):
        """Instead of returning Document instances, return either a specific
        value or a tuple of values in order.

        Can be used along with
        :func:`~mongoengine.queryset.QuerySet.no_dereference` to turn off
        dereferencing.

        .. note:: This effects all results and can be unset by calling
                  ``scalar`` without arguments. Calls ``only`` automatically.

        :param fields: One or more fields to return instead of a Document.
        """
        queryset = self.clone()
        queryset._scalar = list(fields)

        if fields:
            queryset = queryset.only(*fields)
        else:
            queryset = queryset.all_fields()

        return queryset

    def values_list(self, *fields):
        """An alias for scalar"""
        return self.scalar(*fields)

    def as_pymongo(self):
        """Instead of returning Document instances, return raw values from
        pymongo.

        This method is particularly useful if you don't need dereferencing
        and care primarily about the speed of data retrieval.
        """
        queryset = self.clone()
        queryset._as_pymongo = True
        return queryset

    def max_time_ms(self, ms):
        """Wait `ms` milliseconds before killing the query on the server

        :param ms: the number of milliseconds before killing the query on the server
        """
        return self._chainable_method("max_time_ms", ms)

    # JSON Helpers

    async def to_json(self, *args, **kwargs):
        """Converts a queryset to JSON"""
        docs = async_iteritems(self.as_pymongo())
        return json_util.dumps(docs, *args, **kwargs)

    def from_json(self, json_data):
        """Converts json data to unsaved objects"""
        son_data = json_util.loads(json_data)
        return [
            self._document._from_son(data, only_fields=self.only_fields)
            for data in son_data
        ]

    async def aggregate(self, *pipeline, **kwargs):
        """ Perform a aggregate function based in your queryset params

        :param pipeline: list of aggregation commands,\
            see: http://docs.mongodb.org/manual/core/aggregation-pipeline/
        """
        initial_pipeline = []

        if self._query:
            initial_pipeline.append({"$match": self._query})

        if self._ordering:
            initial_pipeline.append({"$sort": dict(self._ordering)})

        if self._limit is not None:
            initial_pipeline.append({"$limit": self._limit + (self._skip or 0)})

        if self._skip is not None:
            initial_pipeline.append({"$skip": self._skip})

        pipeline = initial_pipeline + list(pipeline)

        if self._read_preference is not None:
            return await self._collection \
                .with_options(read_preference=self._read_preference) \
                .aggregate(pipeline, cursor={}, **kwargs)

        return await self._collection.aggregate(pipeline, cursor={}, **kwargs)

    # JS functionality
    def map_reduce(
            self, map_f, reduce_f, output, finalize_f=None, limit=None, scope=None
    ):
        """Perform a map/reduce query using the current query spec
        and ordering. While ``map_reduce`` respects ``QuerySet`` chaining,
        it must be the last call made, as it does not return a maleable
        ``QuerySet``.

        See the :meth:`~mongoengine.tests.QuerySetTest.test_map_reduce`
        and :meth:`~mongoengine.tests.QuerySetTest.test_map_advanced`
        tests in ``tests.queryset.QuerySetTest`` for usage examples.

        :param map_f: map function, as :class:`~bson.code.Code` or string
        :param reduce_f: reduce function, as
                         :class:`~bson.code.Code` or string
        :param output: output collection name, if set to 'inline' will try to
           use :class:`~pymongo.collection.Collection.inline_map_reduce`
           This can also be a dictionary containing output options
           see: http://docs.mongodb.org/manual/reference/command/mapReduce/#dbcmd.mapReduce
        :param finalize_f: finalize function, an optional function that
                           performs any post-reduction processing.
        :param scope: values to insert into map/reduce global scope. Optional.
        :param limit: number of objects from current query to provide
                      to map/reduce method

        Returns an iterator yielding
        :class:`~mongoengine.document.MapReduceDocument`.

        .. note::

            Map/Reduce changed in server version **>= 1.7.4**. The PyMongo
            :meth:`~pymongo.collection.Collection.map_reduce` helper requires
            PyMongo version **>= 1.11**.
        """
        queryset = self.clone()

        MapReduceDocument = _import_class("MapReduceDocument")

        if not hasattr(self._collection, "map_reduce"):
            raise NotImplementedError("Requires MongoDB >= 1.7.1")

        map_f_scope = {}
        if isinstance(map_f, Code):
            map_f_scope = map_f.scope
            map_f = six.text_type(map_f)
        map_f = Code(queryset._sub_js_fields(map_f), map_f_scope)

        reduce_f_scope = {}
        if isinstance(reduce_f, Code):
            reduce_f_scope = reduce_f.scope
            reduce_f = six.text_type(reduce_f)
        reduce_f_code = queryset._sub_js_fields(reduce_f)
        reduce_f = Code(reduce_f_code, reduce_f_scope)

        mr_args = {"query": queryset._query}

        if finalize_f:
            finalize_f_scope = {}
            if isinstance(finalize_f, Code):
                finalize_f_scope = finalize_f.scope
                finalize_f = six.text_type(finalize_f)
            finalize_f_code = queryset._sub_js_fields(finalize_f)
            finalize_f = Code(finalize_f_code, finalize_f_scope)
            mr_args["finalize"] = finalize_f

        if scope:
            mr_args["scope"] = scope

        if limit:
            mr_args["limit"] = limit

        if output == "inline" and not queryset._ordering:
            map_reduce_function = "inline_map_reduce"
        else:
            map_reduce_function = "map_reduce"

            if isinstance(output, six.string_types):
                mr_args["out"] = output

            elif isinstance(output, dict):
                ordered_output = []

                for part in ("replace", "merge", "reduce"):
                    value = output.get(part)
                    if value:
                        ordered_output.append((part, value))
                        break

                else:
                    raise OperationError("actionData not specified for output")

                db_alias = output.get("db_alias")
                remaing_args = ["db", "sharded", "nonAtomic"]

                if db_alias:
                    ordered_output.append(("db", get_db(db_alias).name))
                    del remaing_args[0]

                for part in remaing_args:
                    value = output.get(part)
                    if value:
                        ordered_output.append((part, value))

                mr_args["out"] = SON(ordered_output)

        results = getattr(queryset._collection, map_reduce_function)(
            map_f, reduce_f, **mr_args
        )

        if map_reduce_function == "map_reduce":
            results = results.find()

        if queryset._ordering:
            results = results.sort(queryset._ordering)

        for doc in results:
            yield MapReduceDocument(
                queryset._document, queryset._collection, doc["_id"], doc["value"]
            )

    # def exec_js(self, code, *fields, **options):
    #     """Execute a Javascript function on the server. A list of fields may be
    #     provided, which will be translated to their correct names and supplied
    #     as the arguments to the function. A few extra variables are added to
    #     the function's scope: ``collection``, which is the name of the
    #     collection in use; ``query``, which is an object representing the
    #     current query; and ``options``, which is an object containing any
    #     options specified as keyword arguments.
    #
    #     As fields in MongoEngine may use different names in the database (set
    #     using the :attr:`db_field` keyword argument to a :class:`Field`
    #     constructor), a mechanism exists for replacing MongoEngine field names
    #     with the database field names in Javascript code. When accessing a
    #     field, use square-bracket notation, and prefix the MongoEngine field
    #     name with a tilde (~).
    #
    #     :param code: a string of Javascript code to execute
    #     :param fields: fields that you will be using in your function, which
    #         will be passed in to your function as arguments
    #     :param options: options that you want available to the function
    #         (accessed in Javascript through the ``options`` object)
    #     """
    #     queryset = self.clone()
    #
    #     code = queryset._sub_js_fields(code)
    #
    #     fields = [queryset._document._translate_field_name(f) for f in fields]
    #     collection = queryset._document._get_collection_name()
    #
    #     scope = {"collection": collection, "options": options or {}}
    #
    #     query = queryset._query
    #     if queryset._where_clause:
    #         query["$where"] = queryset._where_clause
    #
    #     scope["query"] = query
    #     code = Code(code, scope=scope)
    #
    #     db = queryset._document._get_db()
    #     return db.eval(code, *fields)

    def where(self, where_clause):
        """Filter ``QuerySet`` results with a ``$where`` clause (a Javascript
        expression). Performs automatic field name substitution like
        :meth:`mongoengine.queryset.Queryset.exec_js`.

        .. note:: When using this mode of query, the database will call your
                  function, or evaluate your predicate clause, for each object
                  in the collection.
        """
        queryset = self.clone()
        where_clause = queryset._sub_js_fields(where_clause)
        queryset._where_clause = where_clause
        return queryset

    # New
    def _insert_unwind(self, field, pipeline):
        """if we're performing a sum over a list field, we sum up all the
        elements in the list, hence we need to $unwind the arrays first. """
        ListField = _import_class("ListField")
        field_parts = field.split(".")
        field_instances = self._document._lookup_field(field_parts)
        if isinstance(field_instances[-1], ListField):
            pipeline.insert(1, {"$unwind": "$" + field})
        return pipeline

    async def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot notation to refer to
            embedded document fields
        """
        db_field = self._fields_to_dbfields([field]).pop()
        pipeline = [
            {"$match": self._query},
            {"$group": {"_id": "sum", "total": {"$sum": "$" + db_field}}},
        ]

        result = [i async for i in self._document._get_collection().aggregate(pipeline)]
        if result:
            return result[0]["total"]
        return 0

    async def average(self, field):
        """Average over the values of the specified field.

        :param field: the field to average over; use dot notation to refer to
            embedded document fields
        """
        db_field = self._fields_to_dbfields([field]).pop()
        pipeline = [
            {"$match": self._query},
            {"$group": {"_id": "avg", "total": {"$avg": "$" + db_field}}},
        ]
        pipeline = self._insert_unwind(field, pipeline)
        result = [i async for i in self._document._get_collection().aggregate(pipeline)]
        if result:
            return result[0]["total"]
        return 0

    # TODO test this.
    def item_frequencies(self, field, normalize=False, map_reduce=True):
        """Returns a dictionary of all items present in a field across
        the whole queried set of documents, and their corresponding frequency.
        This is useful for generating tag clouds, or searching documents.

        .. note::

            Can only do direct simple mappings and cannot map across
            :class:`~mongoengine.fields.ReferenceField` or
            :class:`~mongoengine.fields.GenericReferenceField` for more complex
            counting a manual map reduce call is required.

        If the field is a :class:`~mongoengine.fields.ListField`, the items within
        each list will be counted individually.

        :param field: the field to use
        :param normalize: normalize the results so they add to 1.0
        :param map_reduce: Use map_reduce over exec_js

        """
        if map_reduce:
            return self._item_frequencies_map_reduce(field, normalize=normalize)
        return self._item_frequencies_exec_js(field, normalize=normalize)

    def rewind(self):
        """Rewind the cursor to its unevaluated state."""
        self._iter = False
        self._cursor.rewind()

    # Properties

    @property
    def _collection(self):
        """Property that returns the collection object. This allows us to
        perform operations only if the collection is accessed.
        """
        return self._collection_obj

    @property
    def _cursor_args(self) -> dict:
        fields_name = "projection"
        # snapshot is not handled at all by PyMongo 3+
        # TODO: evaluate similar possibilities using modifiers

        cursor_args = {}
        if not self._timeout:
            cursor_args["no_cursor_timeout"] = True

        if self._loaded_fields:
            cursor_args[fields_name] = self._loaded_fields.as_dict()

        if self._search_text:
            if fields_name not in cursor_args:
                cursor_args[fields_name] = {}

            cursor_args[fields_name]["_text_score"] = {"$meta": "textScore"}

        return cursor_args

    @property
    def _cursor(self) -> 'AgnosticCursor':
        """Return a motor cursor object corresponding to this queryset."""

        # If _cursor_obj already exists, return it immediately.
        if self._cursor_obj is not None:
            return self._cursor_obj

        # Create a new PyMongo cursor.
        # XXX In PyMongo 3+, we define the read preference on a collection
        # level, not a cursor level. Thus, we need to get a cloned collection
        # object using `with_options` first.
        if self._read_preference is not None:
            self._cursor_obj = self._collection.with_options(
                read_preference=self._read_preference
            ).find(self._query, **self._cursor_args)
        else:
            self._cursor_obj = self._collection.find(self._query, **self._cursor_args)

        # Apply "where" clauses to cursor
        if self._where_clause:
            where_clause = self._sub_js_fields(self._where_clause)
            self._cursor_obj.where(where_clause)

        # Apply ordering to the cursor.
        # XXX self._ordering can be equal to:
        # * None if we didn't explicitly call order_by on this queryset.
        # * A list of PyMongo-style sorting tuples.
        # * An empty list if we explicitly called order_by() without any
        #   arguments. This indicates that we want to clear the default
        #   ordering.
        if self._ordering:
            # explicit ordering
            self._cursor_obj.sort(self._ordering)
        elif self._ordering is None and self._document._meta["ordering"]:
            # default ordering
            order = self._get_order_by(self._document._meta["ordering"])
            self._cursor_obj.sort(order)

        if self._limit is not None:
            self._cursor_obj.limit(self._limit)

        if self._skip is not None:
            self._cursor_obj.skip(self._skip)

        if self._hint != -1:
            self._cursor_obj.hint(self._hint)

        if self._collation is not None:
            self._cursor_obj.collation(self._collation)

        if self._batch_size is not None:
            self._cursor_obj.batch_size(self._batch_size)

        if self._comment is not None:
            self._cursor_obj.comment(self._comment)

        return self._cursor_obj

    def __deepcopy__(self, memo):
        """Essential for chained queries with ReferenceFields involved"""
        return self.clone()

    @property
    def _query(self) -> dict:
        if self._mongo_query is None:
            self._mongo_query = self._query_obj.to_query(self._document)
            if self._cls_query:
                if "_cls" in self._mongo_query:
                    self._mongo_query = {"$and": [self._cls_query, self._mongo_query]}
                else:
                    self._mongo_query.update(self._cls_query)
        return self._mongo_query

    # @property
    # def _dereference(self):
    #     if not self.__dereference:
    #         self.__dereference = _import_class("DeReference")()
    #     return self.__dereference
    #
    # def no_dereference(self):
    #     """Turn off any dereferencing for the results of this queryset."""
    #     queryset = self.clone()
    #     queryset._auto_dereference = False
    #     return queryset

    # Helper Functions

    # TODO test this.
    def _item_frequencies_map_reduce(self, field, normalize=False):
        map_func = """
                function() {
                    var path = '{{~%(field)s}}'.split('.');
                    var field = this;

                    for (p in path) {
                        if (typeof field != 'undefined')
                           field = field[path[p]];
                        else
                           break;
                    }
                    if (field && field.constructor == Array) {
                        field.forEach(function(item) {
                            emit(item, 1);
                        });
                    } else if (typeof field != 'undefined') {
                        emit(field, 1);
                    } else {
                        emit(null, 1);
                    }
                }
            """ % {
            "field": field
        }
        reduce_func = """
                function(key, values) {
                    var total = 0;
                    var valuesSize = values.length;
                    for (var i=0; i < valuesSize; i++) {
                        total += parseInt(values[i], 10);
                    }
                    return total;
                }
            """
        values = self.map_reduce(map_func, reduce_func, "inline")
        frequencies = {}
        for f in values:
            key = f.key
            if isinstance(key, float):
                if int(key) == key:
                    key = int(key)
            frequencies[key] = int(f.value)

        if normalize:
            count = sum(frequencies.values())
            frequencies = {k: float(v) / count for k, v in frequencies.items()}

        return frequencies

    # TODO test this.
    def _item_frequencies_exec_js(self, field, normalize=False):
        """Uses exec_js to execute"""
        freq_func = """
                function(path) {
                    var path = path.split('.');

                    var total = 0.0;
                    db[collection].find(query).forEach(function(doc) {
                        var field = doc;
                        for (p in path) {
                            if (field)
                                field = field[path[p]];
                             else
                                break;
                        }
                        if (field && field.constructor == Array) {
                           total += field.length;
                        } else {
                           total++;
                        }
                    });

                    var frequencies = {};
                    var types = {};
                    var inc = 1.0;

                    db[collection].find(query).forEach(function(doc) {
                        field = doc;
                        for (p in path) {
                            if (field)
                                field = field[path[p]];
                            else
                                break;
                        }
                        if (field && field.constructor == Array) {
                            field.forEach(function(item) {
                                frequencies[item] = inc + (isNaN(frequencies[item]) ? 
                                0 : frequencies[item]);
                            });
                        } else {
                            var item = field;
                            types[item] = item;
                            frequencies[item] = inc + (isNaN(frequencies[item]) ? 
                            0 : frequencies[item]);
                        }
                    });
                    return [total, frequencies, types];
                }
            """
        total, data, types = self.exec_js(freq_func, field)
        values = {types.get(k): int(v) for k, v in data.items()}

        if normalize:
            values = {k: float(v) / total for k, v in values.items()}

        frequencies = {}
        for k, v in values.items():
            if isinstance(k, float):
                if int(k) == k:
                    k = int(k)

            frequencies[k] = v

        return frequencies

    def _fields_to_dbfields(self, fields):
        """Translate fields' paths to their db equivalents."""
        subclasses = []
        if self._document._meta["allow_inheritance"]:
            subclasses = [get_document(x) for x in self._document._subclasses][1:]

        db_field_paths = []
        for field in fields:
            field_parts = field.split(".")
            try:
                field = ".".join(
                    f if isinstance(f, six.string_types) else f.db_field
                    for f in self._document._lookup_field(field_parts)
                )
                db_field_paths.append(field)
            except LookUpError as err:
                found = False

                # If a field path wasn't found on the main document, go
                # through its subclasses and see if it exists on any of them.
                for sub_doc in subclasses:
                    try:
                        subfield = ".".join(
                            f if isinstance(f, six.string_types) else f.db_field
                            for f in sub_doc._lookup_field(field_parts)
                        )
                        db_field_paths.append(subfield)
                        found = True
                        break
                    except LookUpError:
                        pass

                if not found:
                    raise err

        return db_field_paths

    def _get_order_by(self, keys):
        """Given a list of MongoEngine-style sort keys, return a list
        of sorting tuples that can be applied to a PyMongo cursor. For
        example:

        >>> qs._get_order_by(['-last_name', 'first_name'])
        [('last_name', -1), ('first_name', 1)]
        """
        key_list = []
        for key in keys:
            if not key:
                continue

            if key == "$text_score":
                key_list.append(("_text_score", {"$meta": "textScore"}))
                continue

            direction = pymongo.ASCENDING
            if key[0] == "-":
                direction = pymongo.DESCENDING

            if key[0] in ("-", "+"):
                key = key[1:]

            key = key.replace("__", ".")
            try:
                key = self._document._translate_field_name(key)
            except Exception:
                # TODO this exception should be more specific
                pass

            key_list.append((key, direction))

        return key_list

    def _get_scalar(self, doc):
        def lookup(obj, name):
            chunks = name.split("__")
            for chunk in chunks:
                obj = getattr(obj, chunk)
            return obj

        data = [lookup(doc, n) for n in self._scalar]
        if len(data) == 1:
            return data[0]

        return tuple(data)

    def _sub_js_fields(self, code):
        """When fields are specified with [~fieldname] syntax, where
        *fieldname* is the Python name of a field, *fieldname* will be
        substituted for the MongoDB name of the field (specified using the
        :attr:`name` keyword argument in a field's constructor).
        """

        def field_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split(".")
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return u'["%s"]' % fields[-1].db_field

        def field_path_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split(".")
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return ".".join([f.db_field for f in fields])

        code = re.sub(r"\[\s*~([A-z_][A-z_0-9.]+?)\s*\]", field_sub, code)
        code = re.sub(r"{{\s*~([A-z_][A-z_0-9.]+?)\s*\}\}", field_path_sub, code)
        return code

    def _chainable_method(self, method_name, val):
        """Call a particular method on the PyMongo cursor call a particular chainable method
        with the provided value.
        """
        queryset = self.clone()

        # Get an existing cursor object or create a new one
        cursor = queryset._cursor

        # Find the requested method on the cursor and call it with the
        # provided value
        getattr(cursor, method_name)(val)

        # Cache the value on the queryset._{method_name}
        setattr(queryset, "_" + method_name, val)

        return queryset
