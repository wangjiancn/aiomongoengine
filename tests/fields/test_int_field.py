import pytest
from aiomongoengine import Document
from aiomongoengine.errors import ValidationError
from aiomongoengine.fields import IntField
from tests.utils import get_as_son

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("value", [-1, 0, 1, None])
async def test_storage(value):
    class TestingUser(Document):
        age = IntField(min_value=-1, max_value=1)

    user = TestingUser(age=value)
    await user.save()
    assert await get_as_son(user) == {'_id': user.id, 'age': value}


TEST_VALIDATE_CASE = [
    (None, 'Field is required'),
    (-1, 'Integer value is too small'),
    (101, 'Integer value is too large'),
]


@pytest.mark.parametrize("value,err_msg", TEST_VALIDATE_CASE)
async def test_validate(value, err_msg):
    class TestingUser(Document):
        age = IntField(max_value=100, min_value=0, required=True)

    user = TestingUser(age=value)
    with pytest.raises(ValidationError) as exc_info:
        user.validate()

    error = exc_info.value.to_dict()
    assert error['age'] == err_msg
