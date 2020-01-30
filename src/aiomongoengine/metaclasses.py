from typing import Any
from typing import Dict
from typing import Tuple
from typing import TYPE_CHECKING

from aiomongoengine.query.queryset import QuerySet

from .errors import InvalidDocumentError
from .fields import BaseField
from .fields import ObjectIdField

if TYPE_CHECKING:
    from .document import Document

registered_collections: Dict[str, Any] = {}


class Meta(dict):
    merge_options = ('indexes',)
    no_merge_options = ('abstract',)

    def merge(self, new_meta):
        if not isinstance(new_meta, dict):
            raise ValueError("meta must be dict.")
        for k, v in new_meta.items():
            if k in self.merge_options:
                self[k] = self.get(k, []) + v
            elif k in self.no_merge_options:
                pass
            else:
                self[k] = v

    def add_index(self, index):
        has_duplicate = False
        indexes = self.get('indexes', [])
        for i in indexes:
            if i == index:
                has_duplicate = True
                break
        if not has_duplicate:
            indexes.append(index)
            self['indexes'] = indexes


class ClassProperty(property):
    def __get__(self, instance, owner):
        return classmethod(self.fget).__get__(None, owner)()


class DocumentMetaClass(type):

    def __new__(mcs, name, bases, attrs):
        """元类

        :param name:(str) 类名,如:User
        :param bases:(tuple) 继承元类的类
        :param attrs:(dict) 类属性,如:name,__module__,__qualname__等
        :raises InvalidDocumentError: 非法Document
        :return:(cls) 新的Document,如:User
        """
        flattened_bases = mcs._get_bases(bases)
        super_new = super(DocumentMetaClass, mcs).__new__

        # storage doc_fields, eg.{'name': <fields.string_field.StringField>}
        meta = Meta(
            id_field=None,
            indexes=[],
            ordering=[],
            allow_inheritance=False,
            abstract=False
        )
        meta.update(attrs.pop('meta', {}))

        doc_fields = {}
        for base in flattened_bases[::-1]:
            if hasattr(base, '_fields'):
                doc_fields.update(base._fields)
            if hasattr(base, '_meta'):
                meta.merge(base._meta)

        field_names = {}

        for field_name, field in doc_fields.items():
            field_names[field.db_field] = field_names.get(field.db_field, 0) + 1

        for attr_name, attr_value in attrs.items():
            if not isinstance(attr_value, BaseField):
                continue
            if attr_value.__class__.__name__ == 'DynamicField':
                continue
            attr_value.name = attr_name
            if not attr_value.db_field:
                attr_value.db_field = attr_name
            doc_fields[attr_name] = attr_value

            if attr_value.unique or attr_value.sparse:
                meta['indexes'].append({
                    'fields': [attr_name],
                    'unique': attr_value.unique,
                    'sparse': attr_value.sparse
                })

            # Count names to ensure no db_field redefinitions
            field_names[attr_value.db_field] = field_names.get(
                attr_value.db_field, 0) + 1

        duplicate_db_fields = [k for k, v in field_names.items() if v > 1]
        if duplicate_db_fields:
            msg = ("Multiple db_fields defined for: %s " %
                   ", ".join(duplicate_db_fields))
            raise InvalidDocumentError(msg)

        if not meta.get("id_field"):
            id_name, id_db_name = 'id', '_id'
            meta["id_field"] = id_name
            doc_fields[id_name] = ObjectIdField(db_field=id_db_name)
            doc_fields[id_name].name = id_name
            attrs['id'] = doc_fields[id_name]

        if not meta.get('abstract'):
            cls_name = name
            new_cls_name = cls_name[0].lower()
            for s in cls_name[1:]:
                if s.isupper():
                    new_cls_name += f'_{s.lower()}'
                else:
                    new_cls_name += s
            attrs['__collection__'] = new_cls_name
        attrs['_collection'] = None
        attrs['_meta'] = meta
        attrs['_fields'] = doc_fields
        attrs['_db_field_map'] = {k: v.db_field for k, v in doc_fields.items()}
        attrs['_fields_ordered'] = (meta.get('id_field'),) + tuple(
            i[1] for i in sorted((v.creation_counter, v.name) for v in
                                 doc_fields.values()) if
            i[1] != meta.get('id_field'))
        attrs['_reverse_db_field_map'] = dict(
            (v, k) for k, v in attrs['_db_field_map'].items())
        attrs['objects'] = ClassProperty(
            lambda *args, **kw: QuerySet(new_class))

        new_class = super_new(mcs, name, bases, attrs)
        if hasattr(new_class, '__collection__'):
            registered_collections[new_class.__collection__] = new_class

        return new_class

    @classmethod
    def _get_bases(mcs, bases) -> Tuple['Document', ...]:
        """获取一个不重复基类组成的元组"

        :param bases:(bases) 基类元组
        :return:(BasesTuple) 自定义元组
        """
        if isinstance(bases, BasesTuple):
            return bases
        seen = []
        bases = mcs.__get_bases(bases)
        unique_bases = (b for b in bases if not (b in seen or seen.append(b)))
        return BasesTuple(unique_bases)

    @classmethod
    def __get_bases(mcs, bases):
        """递归查找基类中的基类,迭代器

        :param bases:(base) 基类
        """
        for base in bases:
            if base is object:
                continue
            yield base
            for child_base in mcs.__get_bases(base.__bases__):
                yield child_base


class BasesTuple(tuple):
    """Special class to handle introspection of bases tuple in __new__"""
    pass
