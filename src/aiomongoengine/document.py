from __future__ import annotations

from typing import Dict
from typing import NoReturn
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union

from aiomongoengine.errors import PartlyLoadedDocumentError
from aiomongoengine.errors import ValidationError
from aiomongoengine.query.queryset import QuerySet

from .connection import get_connection
from .metaclasses import DocumentMetaClass
from .utils import parse_indexes

if TYPE_CHECKING:
    from bson import ObjectId
    from motor.core import AgnosticCollection
    from motor.core import AgnosticClient
    from .fields.base_field import BaseField

AUTHORIZED_FIELDS = [
    '_id', '_data', '_reference_loaded_fields', 'is_partly_loaded'
]


class BaseDocument(object):
    id: ObjectId
    objects: QuerySet
    _meta: dict
    _fields: Dict[str, 'BaseField']
    _db_field_map: Dict[str, str]
    _fields_ordered: Tuple[str]
    _reverse_db_field_map: Dict[str, str]
    _collection: AgnosticClient
    _class_name: str
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
        from .fields.dynamic_field import DynamicField

        self._data = {}  # storage document field and value
        self.is_partly_loaded = _is_partly_loaded
        self._reference_loaded_fields = _reference_loaded_fields or {}

        for key, value in kw.items():
            if key not in self._fields:
                self._fields[key] = DynamicField(db_field=key)
                setattr(self, key, DynamicField(db_field=key))
            setattr(self, key, value)

    @classmethod
    def close(cls, connection=None):
        """ Set _collection None when call `aiomongoengine.disconnect()` """
        if cls._collection is None:
            return
        if cls._collection.database.client == connection:
            cls._collection = None

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
                 dic,
                 _is_partly_loaded=False,
                 _reference_loaded_fields=None):
        field_values = {}
        for name, value in dic.items():
            field = cls.get_field_by_db_name(name)
            if field:
                field_values[field.name] = field.from_son(value)
            else:
                field_values[name] = value

        return cls(
            _is_partly_loaded=_is_partly_loaded,
            _reference_loaded_fields=_reference_loaded_fields,
            **field_values
        )

    def to_son(self, fields=None, on_save=False) -> dict:
        """ Instance to bson"""
        fields = fields or []
        root_fields = {f.split('.')[0] for f in fields}

        data = {}

        for name, field in self._fields.items():
            if root_fields and name not in root_fields:
                continue
            value = self.get_field_value(name, on_save=on_save)
            if field.sparse and value is None:
                continue
            data[field.db_field] = field.to_son(value)
        if self.id is None:
            del data['_id']
        return data

    def validate(self) -> NoReturn:
        """validate all field"""
        errors = {}

        for name, field in self._fields.items():
            value = self.get_field_value(name)
            if not field.is_empty(value):
                try:
                    field.validate(value)
                except ValidationError as error:
                    errors[field.name] = error.errors or error
                except (ValueError, AssertionError, AttributeError) as error:
                    errors[field.name] = error
            elif field.required:
                errors[field.name] = ValidationError(
                    message="Field is required", field_name=field.name
                )
        if errors:
            pk = "None"
            if hasattr(self, "id"):
                pk = self.id
            message = f"ValidationError {self._class_name}:{pk}"
            raise ValidationError(message=message, errors=errors)

    def get_field_value(self, name, on_save=False):
        """ Get field's value. """
        if name not in self._fields:
            raise ValueError("Field %s not found in instance of %s." % (
                name,
                self.__class__.__name__
            ))
        field = self._fields[name]
        if field.db_field in self._data:
            value = self._data.get(field.db_field)
        else:
            value = field.get_value(None)
        if on_save:
            value = field.get_db_prep_value(value)
        return value

    @classmethod
    def get_field_by_db_name(cls, name) -> Union['BaseField', None]:
        """ Get field by BaseField.db_field"""
        for field_name, field in cls._fields.items():
            if name == field.db_field or name.lstrip("_") == field.db_field:
                return field
        return None

    @classmethod
    def get_fields(cls, name, fields=None):

        from .fields.dynamic_field import DynamicField

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
                   validate: bool = True,
                   alias: str = None,
                   upsert: bool = False):
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

        if validate:
            self.validate()

        doc = self.to_son(on_save=True)
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
        self._data.update(doc)
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
    def from_dict(cls, doc: dict) -> Union['Document']:
        return cls(**doc)
