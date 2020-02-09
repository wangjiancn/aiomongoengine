import pytest
from aiomongoengine import Document
from aiomongoengine.errors import ValidationError
from aiomongoengine.fields import BooleanField
from tests.utils import get_as_son

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("value", [False, True, None])
async def test_storage(value):
    class TestingUser(Document):
        is_staff = BooleanField()

    user = TestingUser(is_staff=value)
    await user.save()
    assert await get_as_son(user) == {'_id': user.id, 'is_staff': value}


TEST_VALIDATE_CASE = [
    ('true', 'BooleanField only accepts boolean values', {}),
    (None, 'Field is required', {'required': True})
]


@pytest.mark.parametrize("value,err_msg,field_kw", TEST_VALIDATE_CASE)
async def test_validate(value, err_msg, field_kw):
    class TestingUser(Document):
        is_staff = BooleanField(**field_kw)

    user = TestingUser(is_staff=value)
    with pytest.raises(ValidationError) as exc_info:
        user.validate()

    error = exc_info.value.to_dict()
    assert error['is_staff'] == err_msg
