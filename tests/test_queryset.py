import pytest
from aiomongoengine import Q
from aiomongoengine import QNot



@pytest.mark.parametrize(
    "query,count",
    [
        (dict(name__icontains='lisa'), 2),
        (dict(name__istartswith='l'), 3),
        (dict(name__istartswith='l', age__gt=30), 2)
    ]
)
@pytest.mark.asyncio
async def test_filter(user_cls, query, count):
    ret = await user_cls.objects.filter(**query).count()
    assert ret == count
