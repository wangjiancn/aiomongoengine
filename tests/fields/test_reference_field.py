import pytest
from aiomongoengine import Document
from aiomongoengine import StringField
from aiomongoengine.errors import ValidationError
from aiomongoengine.fields import ReferenceField

from tests.utils import get_as_son

pytestmark = pytest.mark.asyncio


class Other(Document):
    other = StringField(default='other')


class Role(Document):
    name = StringField()


class TestingUser(Document):
    name = StringField()
    role = ReferenceField(Role)


async def test_storage():
    role = Role(name='admin')
    await role.save()
    user = TestingUser(name='test', role=role)
    await user.save()
    assert await get_as_son(user) == {
        '_id': user.id,
        'name': 'test',
        'role': role.id
    }


TEST_VALIDATE_CASE = [
    ('true', 'ReferenceField only accepts ObjectId or Document.', {}),
    (Other(), 'ReferenceField only accepts ObjectId or Document.', {})
]


@pytest.mark.parametrize("value,err_msg,field_kw", TEST_VALIDATE_CASE)
async def test_validate(value, err_msg, field_kw):
    user = TestingUser(name='test', role=value)
    with pytest.raises(ValidationError) as exc_info:
        user.validate()
    error = exc_info.value.to_dict()
    assert error['role'] == err_msg


async def test_not_save_reference():
    role = Role(name='admin')
    user = TestingUser(name='test', role=role)
    with pytest.raises(ValidationError) as exc_info:
        user.validate()
    a = exc_info
    error = exc_info.value.to_dict()
    assert error['role'] == 'You can only reference documents once they ' \
                            'have been saved to the database'
