from __future__ import annotations
from typing import Dict, List, Tuple

from .queryset import QuerySet
from .fields import BaseField
import six
from .metaclasses import DocumentMetaClass, registered_collectinos
from .errors import InvalidDocumentError, LoadReferencesRequiredError


AUTHORIZED_FIELDS = [
    '_id', '_values', '_reference_loaded_fields', 'is_partly_loaded'
]


def get_collections() -> Dict[str, Document]:
    """获取所有集合

    :return:(dict) name/Document 键值对
    """
    return registered_collectinos


def get_collection_list() -> List[Document]:
    """获取所有集合组成的列表

    :return:(list) [Document1,Document2,...]
    """
    collection_list = list(registered_collectinos.values())
    return collection_list


class BaseDocument(object):

    objects: QuerySet
    _fields: Dict[str, BaseField]
    _db_field_map: Dict[str, str]
    _fields_ordered: Tuple[str]
    _reverse_db_field_map: Dict[str, str]

    def __init__(
        self, _is_partly_loaded=False, _reference_loaded_fields=None, **kw
    ):
        """
        :param _is_partly_loaded: is a flag that indicates if the document was
        loaded partly (with `only`, `exlude`, `fields`). Default: False.
        :param _reference_loaded_fields: dict that contains projections for
        reference fields if any. Default: None.
        :param kw: pairs of fields of the document and their values
        """

        self._values = {}   # storage document field and value
        _id = kw.pop('_id', None) or kw.pop('id', None)
        self._id = _id  # cache primary key _id
        self.id = _id
        self.is_partly_loaded = _is_partly_loaded
        self._reference_loaded_fields = _reference_loaded_fields or {}

        # set default value
        for key, field in self._fields.items():
            if callable(field.default):
                self._values[field.name] = field.default()
            else:
                self._values[field.name] = field.default

        # TODO add DynamicDocument
        # set dynamicfield value

        from .fields.dynamic_field import DynamicField
        for key, value in kw.items():
            if key not in self._fields:
                self._fields[key] = DynamicField(
                    db_field="_%s" % key.lstrip('_'))
            self._values[key] = value

    @classmethod
    async def ensure_index(cls):
        cls.objects.ensure_index()

    @property
    def is_lazy(self):
        return self.__class__.__lazy__

    def is_list_field(self, field):
        from .fields.list_field import ListField
        return isinstance(field, ListField) or (isinstance(field, type) and issubclass(field, ListField))

    def is_reference_field(self, field):
        from .fields.reference_field import ReferenceField
        return isinstance(field, ReferenceField) or (isinstance(field, type) and issubclass(field, ReferenceField))

    def is_embedded_field(self, field):
        from .fields.embedded_document_field import EmbeddedDocumentField
        return isinstance(field, EmbeddedDocumentField) or (isinstance(field, type) and issubclass(field, EmbeddedDocumentField))

    @classmethod
    def from_son(cls, dic, _is_partly_loaded=False, _reference_loaded_fields=None):
        field_values = {}
        _object_id = dic.pop('_id', None)
        for name, value in dic.items():
            field = cls.get_field_by_db_name(name)
            if field:
                field_values[field.name] = field.from_son(value)
            else:
                field_values[name] = value
        field_values["_id"] = _object_id

        return cls(
            _is_partly_loaded=_is_partly_loaded,
            _reference_loaded_fields=_reference_loaded_fields,
            **field_values
        )

    # TODO 处理id和_id
    def to_son(self):
        """instance to bson"""
        data = dict()

        for name, field in self._fields.items():
            value = self.get_field_value(name)
            if field.sparse and value is None:
                continue
            data[field.db_field] = field.to_son(value)
        if self._id:
            data['_id'] = self._id
        return data

    def to_dict(self):
        """instance to dict"""
        data = self._values
        data['id'] = self.id
        return self._values

    @classmethod
    def from_dict(cls, dict):
        if dict.get('id'):
            dict['_id'] = dict['id']
        return cls(**dict)

    def validate(self):
        """validate all filed"""
        return self.validate_fields()

    def validate_fields(self):
        for name, field in self._fields.items():

            value = self.get_field_value(name)

            if field.required and field.is_empty(value):
                raise InvalidDocumentError("Field '%s' is required." % name)
            if not field.validate(value):
                raise InvalidDocumentError("Field '%s' must be valid." % name)

        return True

    async def save(self, upsert=False):
        """
        Creates or updates the current instance of this document.
        """
        return await self.objects.save(self, upsert=upsert)

    async def delete(self, alias=None):
        """ Deletes the current instance of this Document."""
        return await self.objects.filter(_id=self._id).delete()

    def fill_values_collection(self, collection, field_name, value):
        collection[field_name] = value

    def fill_list_values_collection(self, collection, field_name, value):
        if field_name not in collection:
            collection[field_name] = []
        collection[field_name].append(value)

    async def reload(self):
        """刷新对象"""
        id = self.id or self._id
        if id:
            return await self.objects.get(_id=id)
        else:
            return None

    def handle_load_reference(self, callback, references, reference_count, values_collection, field_name, fill_values_method=None):
        if fill_values_method is None:
            fill_values_method = self.fill_values_collection

        def handle(*args, **kw):
            fill_values_method(values_collection, field_name, args[0])

            if reference_count > 0:
                references.pop()

            if len(references) == 0:
                callback({
                    'loaded_reference_count': reference_count,
                    'loaded_values': values_collection
                })

        return handle

    async def load_references(self, fields=None, callback=None, alias=None):
        if callback is None:
            raise ValueError("Callback can't be None")

        references = self.find_references(document=self, fields=fields)
        reference_count = len(references)

        if not reference_count:
            callback({
                'loaded_reference_count': reference_count,
                'loaded_values': []
            })
            return

        for dereference_function, document_id, values_collection, field_name, fill_values_method in references:
            dereference_function(
                document_id,
                callback=self.handle_load_reference(
                    callback=callback,
                    references=references,
                    reference_count=reference_count,
                    values_collection=values_collection,
                    field_name=field_name,
                    fill_values_method=fill_values_method
                )
            )

    def find_references(self, document, fields=None, results=None):
        if results is None:
            results = []

        if not isinstance(document, Document):
            return results

        if fields:
            fields = [
                (field_name, field)
                for field_name, field in document._fields.items()
                if field_name in fields
            ]
        else:
            fields = [field for field in document._fields.items()]

        for field_name, field in fields:
            self.find_reference_field(document, results, field_name, field)
            self.find_list_field(document, results, field_name, field)
            self.find_embed_field(document, results, field_name, field)

        return results

    def _get_load_function(self, document, field_name, document_type):
        """Get appropriate method to load reference field of the document"""
        if field_name in document._reference_loaded_fields:
            # there is a projection for this field
            fields = document._reference_loaded_fields[field_name]
            return document_type.objects.fields(**fields).get
        return document_type.objects.get

    def find_reference_field(self, document, results, field_name, field):
        if self.is_reference_field(field):
            value = document._values.get(field_name, None)
            load_function = self._get_load_function(
                document, field_name, field.reference_type
            )
            if value is not None:
                results.append([
                    load_function,
                    value,
                    document._values,
                    field_name,
                    None
                ])

    def find_list_field(self, document, results, field_name, field):
        from .fields.reference_field import ReferenceField
        if self.is_list_field(field):
            values = document._values.get(field_name)
            if values:
                document_type = values[0].__class__
                if isinstance(field._base_field, ReferenceField):
                    document_type = field._base_field.reference_type
                    load_function = self._get_load_function(
                        document, field_name, document_type
                    )
                    for value in values:
                        results.append([
                            load_function,
                            value,
                            document._values,
                            field_name,
                            self.fill_list_values_collection
                        ])
                    document._values[field_name] = []
                else:
                    self.find_references(
                        document=document_type, results=results)

    def find_embed_field(self, document, results, field_name, field):
        if self.is_embedded_field(field):
            value = document._values.get(field_name, None)
            if value:
                self.find_references(document=value, results=results)

    def get_field_value(self, name):
        if name not in self._fields:
            raise ValueError("Field %s not found in instance of %s." % (
                name,
                self.__class__.__name__
            ))

        field = self._fields[name]
        value = field.get_value(self._values.get(name, None))

        return value

    def __getitem__(self, name):
        return self.__getattribute__(name)

    def __getattribute__(self, name):
        # required for the next test
        if name in ['_fields']:
            return object.__getattribute__(self, name)

        if name == 'id':
            return object.__getattribute__(self, '_id')

        if name in self._fields:
            field = self._fields[name]
            is_reference_field = self.is_reference_field(field)
            value = field.get_value(self._values.get(name, None))

            if is_reference_field and value is not None and not isinstance(value, field.reference_type):
                message = "The property '%s' can't be accessed before calling 'load_references'" + \
                    " on its instance first (%s) or setting __lazy__ to False in the %s class."

                raise LoadReferencesRequiredError(
                    message % (name, self.__class__.__name__,
                               self.__class__.__name__)
                )

            return value

        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        from .fields.dynamic_field import DynamicField

        if name not in AUTHORIZED_FIELDS and name not in self._fields:
            self._fields[name] = DynamicField(db_field="_%s" % name)

        if name in self._fields:
            self._values[name] = value
            return

        object.__setattr__(self, name, value)

    @classmethod
    def get_field_by_db_name(cls, name):
        for field_name, field in cls._fields.items():
            if name == field.db_field or name.lstrip("_") == field.db_field:
                return field
        return None

    @classmethod
    def get_fields(cls, name, fields=None):
        from  import EmbeddedDocumentField, ListField
        from .fields.dynamic_field import DynamicField

        if fields is None:
            fields = []

        if '.' not in name:
            dyn_field = DynamicField(db_field="_%s" % name)
            fields.append(cls._fields.get(name, dyn_field))
            return fields

        field_values = name.split('.')
        dyn_field = DynamicField(db_field="_%s" % field_values[0])
        obj = cls._fields.get(field_values[0], dyn_field)
        fields.append(obj)

        if isinstance(obj, (EmbeddedDocumentField, )):
            obj.embedded_type.get_fields(
                ".".join(field_values[1:]), fields=fields)

        if isinstance(obj, (ListField, )):
            obj.item_type.get_fields(".".join(field_values[1:]), fields=fields)

        return fields


class Document(BaseDocument, metaclass=DocumentMetaClass):
    """Base class for all documents specified in ."""
    pass
