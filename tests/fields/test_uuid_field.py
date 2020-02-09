from uuid import UUID
from uuid import uuid4

import pytest
from aiomongoengine import Document
from aiomongoengine.errors import ValidationError
from aiomongoengine.fields import UUIDField
from tests.utils import get_as_son

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "value", [
        uuid4(),
        UUID('1cac7359-f9e8-4095-8731-6bd5e159b331'),
        '1cac7359-f9e8-4095-8731-6bd5e159b331'
    ])
async def test_storage(value):
    class TestingUser(Document):
        uuid = UUIDField()
        uuid_str = UUIDField(binary=False)

    user = TestingUser(uuid=value, uuid_str=value)
    await user.save()
    data = await get_as_son(user)
    assert data == {
        '_id': user.id,
        'uuid': UUID(str(value)),
        'uuid_str': str(value)
    }


TEST_VALIDATE_CASE = [
    (None, 'Field is required'),
    (1, "Could not convert to UUID: 'int' object has no attribute 'replace'"),
    ('invalid uuid', "Could not convert to UUID: badly formed hexadecimal"
                     " UUID string")
]


@pytest.mark.parametrize("value,err_msg", TEST_VALIDATE_CASE)
async def test_validate(value, err_msg):
    class TestingUser(Document):
        uuid = UUIDField(required=True)

    user = TestingUser(uuid=value)
    with pytest.raises(ValidationError) as exc_info:
        user.validate()

    error = exc_info.value.to_dict()
    assert error['uuid'] == err_msg
