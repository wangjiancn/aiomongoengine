import pytest
from aiomongoengine import Document
from aiomongoengine.errors import ValidationError
from aiomongoengine.fields import StringField
from tests.utils import get_as_son

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "value,db_value", [
        ('', ''),
        (1, '1'),
        (True, 'True'),
        (None, None),
        ('peter', 'peter')])
async def test_storage(value, db_value):
    class TestingUser(Document):
        name = StringField()

    user = TestingUser(name=value)
    await user.save()
    assert await get_as_son(user) == {'_id': user.id, 'name': db_value}


TEST_VALIDATE_CASE = [
    (None, 'Field is required', {'required': True})
]


@pytest.mark.parametrize("value,err_msg,field_kw", TEST_VALIDATE_CASE)
async def test_validate(value, err_msg, field_kw):
    class TestingUser(Document):
        name = StringField(**field_kw)

    user = TestingUser(name=value)
    with pytest.raises(ValidationError) as exc_info:
        user.validate()

    error = exc_info.value.to_dict()
    assert error['name'] == err_msg
