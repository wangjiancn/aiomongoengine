from uuid import uuid4

import pytest
from aiomongoengine import BooleanField
from aiomongoengine import Document
from aiomongoengine import IntField
from aiomongoengine import StringField
from aiomongoengine import UUIDField
from aiomongoengine.fields import ListField

from tests.utils import get_as_son

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "field,value", [
        (StringField(), None)
        # (IntField(), [1, 2, 3, None]),
        # (UUIDField(), [uuid4(), uuid4(), uuid4(), None]),
        # (BooleanField(), [True, False, None]),
        # (StringField(), []),
        # (StringField(), ['v1', 'v2', 'v3', None]),
    ]
)
async def test_storage(field, value):
    class TestingGroup(Document):
        members = ListField(field)

    group = TestingGroup()
    group.validate()
    # await group.save()
    # assert await get_as_son(group) == {'_id': group.id, 'members': value}
