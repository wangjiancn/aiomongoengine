from __future__ import annotations

from typing import Dict
from typing import List
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

from aiomongoengine.errors import PartlyLoadedDocumentError

from .connection import get_connection
from .errors import InvalidDocumentError
from .fields import BaseField
from .fields.dynamic_field import DynamicField
from .metaclasses import DocumentMetaClass
from .metaclasses import registered_collections
from aiomongoengine.query.queryset import QuerySet
from .utils import parse_indexes

if TYPE_CHECKING:
    from bson import ObjectId
    from motor.core import AgnosticCollection

AUTHORIZED_FIELDS = [
    '_id', '_data', '_reference_loaded_fields', 'is_partly_loaded'
]


def get_collections() -> Dict[str, Document]:
    """获取所有集合

    :return:(dict) name/Document 键值对
    """
    return registered_collections


def get_collection_list() -> List[Document]:
    """获取所有集合组成的列表

    :return:(list) [Document1,Document2,...]
    """
    collection_list = list(registered_collections.values())
    return collection_list


class BaseDocument(object):
    id: ObjectId
    objects: QuerySet
    _meta: dict
    _fields: Dict[str, BaseField]
    _db_field_map: Dict[str, str]
    _fields_ordered: Tuple[str]
    _reverse_db_field_map: Dict[str, str]
    __collection__: str

    def __init__(self,
                 _is_partly_loaded=False,
                 _reference_loaded_fields=None,
                 **kw):
        """
        :param _is_partly_loaded: is a flag that indicates if the document was
        loaded partly (with `only`, `exlude`, `fields`). Default: False.
        :param _reference_loaded_fields: dict that contains projections for
        reference fields if any. Default: None.
        :param kw: pairs of fields of the document and their values
        """

        self._data = {}  # storage document field and value
        self.is_partly_loaded = _is_partly_loaded
        self._reference_loaded_fields = _reference_loaded_fields or {}

        for key, value in kw.items():
            if key not in self._fields:
                self._fields[key] = DynamicField()
                setattr(self, key, DynamicField())
            setattr(self, key, value)

    @classmethod
    def _get_collection(cls, alias: str = None) -> AgnosticCollection:
        """Get motor collection class"""
        if not cls._collection:
            if alias is not None:
                db = get_connection(alias=alias)
            else:
                db = get_connection()
            collection = db[cls.__collection__]
            cls._collection = collection
        return cls._collection

    @classmethod
    def from_son(cls,
                 dic, _is_partly_loaded=False,
                 _reference_loaded_fields=None):
        field_values = {}
        _object_id = dic.pop('_id', None)
        for name, value in dic.items():
            field = cls.get_field_by_db_name(name)
            if field:
                field_values[field.name] = field.from_son(value)
            else:
                field_values[name] = value
        field_values["id"] = _object_id

        return cls(
            _is_partly_loaded=_is_partly_loaded,
            _reference_loaded_fields=_reference_loaded_fields,
            **field_values
        )

    def to_son(self):
        """instance to bson"""
        data = dict()

        for name, field in self._fields.items():
            value = self.get_field_value(name)
            if field.sparse and value is None:
                continue
            data[field.db_field] = field.to_son(value)
        if self.id is None:
            del data['_id']
        return data

    def validate(self) -> bool:
        """validate all filed"""
        return self.validate_fields()

    def validate_fields(self) -> bool:
        for name, field in self._fields.items():
            value = self.get_field_value(name)
            if field.required and field.is_empty(value):
                raise InvalidDocumentError("Field '%s' is required." % name)
            if not field.validate(value):
                raise InvalidDocumentError("Field '%s' must be valid." % name)

        return True

    def get_field_value(self, name):
        if name not in self._fields:
            raise ValueError("Field %s not found in instance of %s." % (
                name,
                self.__class__.__name__
            ))
        field = self._fields[name]
        value = field.get_value(self._data.get(name, None))
        return value

    @classmethod
    def get_field_by_db_name(cls, name) -> Union[BaseField, None]:
        for field_name, field in cls._fields.items():
            if name == field.db_field or name.lstrip("_") == field.db_field:
                return field
        return None

    @classmethod
    def get_fields(cls, name, fields=None):

        if fields is None:
            fields = []

        if '.' not in name:
            dyn_field = DynamicField(db_field=name)
            fields.append(cls._fields.get(name, dyn_field))
            return fields

        field_values = name.split('.')
        dyn_field = DynamicField(db_field="%s" % field_values[0])
        obj = cls._fields.get(field_values[0], dyn_field)
        fields.append(obj)
        return fields

    def __getitem__(self, name):
        return self.__getattribute__(name)


class Document(BaseDocument, metaclass=DocumentMetaClass):
    """Base class for all documents specified in ."""

    meta = {'abstract': True}

    async def save(self,
                   alias: str = None,
                   upsert: bool = False) -> Document:
        """ Creates or updates the current instance of this document. """
        if self.is_partly_loaded:
            msg = (
                "Partly loaded document {0} can't be saved. Document should "
                "be loaded without 'only', 'exclude' or 'fields' "
                "QuerySet's modifiers"
            )
            raise PartlyLoadedDocumentError(
                msg.format(self.__class__.__name__)
            )

        if self.validate():
            doc = self.to_son()
            _id = doc.pop('_id', None)
            if _id is not None:
                await self._get_collection(alias).find_one_and_update(
                    {'_id': _id},
                    {'$set': doc},
                    upsert=upsert
                )
            else:
                ret = await self._get_collection(alias).insert_one(doc)
                self.id = ret.inserted_id
            return self

    async def update(self, **kwargs):
        return await self.objects.filter(id=self.id).update(**kwargs)

    async def delete(self, alias=None):
        """ Deletes the current instance of this Document. """
        return await self.objects.filter(id=self.id).delete()

    async def reload(self):
        """刷新对象"""
        if self.id:
            obj = await self.objects.get(id=self.id)
            self._data = obj._data
            return self
        else:
            return self

    @classmethod
    async def ensure_index(cls, alias=None, **kwargs):
        indexes = parse_indexes(cls._meta['indexes'])
        ret = await cls._get_collection(alias).create_indexes(indexes, **kwargs)
        return ret

    @classmethod
    async def drop_collection(cls, alias: str = None):
        return await cls._get_collection(alias=alias).drop()

    def to_dict(self):
        """instance to dict"""
        return self._data

    @classmethod
    def from_dict(cls, doc: dict) -> 'Document':
        return cls(**doc)
