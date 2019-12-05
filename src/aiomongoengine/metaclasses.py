from __future__ import annotations
from typing import Dict, Any

from .fields import BaseField
from .errors import InvalidDocumentError
from .queryset import QuerySet

# code adapted from https://github.com/MongoEngine/mongoengine/blob/master/mongoengine/base/metaclasses.py
# https://docs.python.org/3/library/functions.html#property
# https://docs.python.org/3/library/functions.html#classmethod


registered_collectinos: Dict[str, Any] = {}


class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class DocumentMetaClass(type):

    def __new__(cls, name, bases, attrs):
        """元类

        :param type:(type) 元类自身
        :param name:(str) 类名,如:User
        :param bases:(tuple) 继承元类的类
        :param attrs:(dict) 类属性,如:name,__module__,__qualname__等
        :raises InvalidDocumentError: 非法Document
        :return:(cls) 新的Document,如:User
        """
        flattened_bases = cls._get_bases(bases)
        super_new = super(DocumentMetaClass, cls).__new__

        # storage doc fileds, eg.{'name': <.fields.string_field.StringField object at 0x7f4c79948fd0>}
        doc_fields = {}
        for base in flattened_bases[::-1]:
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)

        # Discover any document fields, eg.{'姓名': 1} , db_field/value pair
        field_names = {}

        for field_name, doc_field in doc_fields.items():
            field_names[doc_field.db_field] = field_names.get(
                doc_field.db_field, 0) + 1

        for attr_name, attr_value in attrs.items():
            if not isinstance(attr_value, BaseField):
                continue
            if attr_value.__class__.__name__ == 'DynamicField':
                continue
            attr_value.name = attr_name
            if not attr_value.db_field:
                attr_value.db_field = attr_name
            doc_fields[attr_name] = attr_value

            # Count names to ensure no db_field redefinitions
            field_names[attr_value.db_field] = field_names.get(
                attr_value.db_field, 0) + 1

        # Ensure no duplicate db_fields
        duplicate_db_fields = [k for k, v in field_names.items() if v > 1]
        if duplicate_db_fields:
            msg = ("Multiple db_fields defined for: %s " %
                   ", ".join(duplicate_db_fields))
            raise InvalidDocumentError(msg)

        # Set _fields, eg.{'name': <.fields.string_field.StringField object at 0x7f7473ff2710> }
        attrs['_fields'] = doc_fields
        # set _db_field_map, eg.{'name': '姓名'}
        attrs['_db_field_map'] = dict([(k, getattr(v, 'db_field', k))
                                       for k, v in doc_fields.items()])
        # set _fields_ordered ('name',)
        attrs['_fields_ordered'] = tuple(i[1] for i in sorted(
                                         (v.creation_counter, v.name)
                                         for v in doc_fields.values()))
        # set _reverse_db_field_map, eg.{'姓名': 'name'}
        attrs['_reverse_db_field_map'] = dict(
            (v, k) for k, v in attrs['_db_field_map'].items())

        new_class = super_new(cls, name, bases, attrs)

        # collection name
        if '__collection__' not in attrs:
            new_class.__collection__ = new_class.__name__.lower()

        if '__lazy__' not in attrs:
            new_class.__lazy__ = True

        if '__alias__' not in attrs:
            new_class.__alias__ = None

        # set queryset as objects
        # objects is a property wrapper with classmethod, can call Class.obejct
        # https://stackoverflow.com/questions/128573/using-property-on-classmethods/13624858

        setattr(new_class, 'objects', classproperty(
            lambda *args, **kw: QuerySet(new_class)))

        if new_class.__name__ != 'Document':
            registered_collectinos[new_class.__collection__] = new_class

        return new_class

    @classmethod
    def _get_bases(cls, bases):
        """获取一个不重复基类组成的元组"

        :param bases:(bases) 基类元组
        :return:(BasesTuple) 自定义元组
        """
        if isinstance(bases, BasesTuple):
            return bases
        seen = []
        bases = cls.__get_bases(bases)
        unique_bases = (b for b in bases if not (b in seen or seen.append(b)))
        return BasesTuple(unique_bases)

    @classmethod
    def __get_bases(cls, bases):
        """递归查找基类中的基类,迭代器

        :param bases:(base) 基类
        """
        for base in bases:
            if base is object:
                continue
            yield base
            for child_base in cls.__get_bases(base.__bases__):
                yield child_base


class BasesTuple(tuple):
    """Special class to handle introspection of bases tuple in __new__"""
    pass
