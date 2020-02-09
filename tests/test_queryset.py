import pytest
from bson import ObjectId

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "query,count",
    [
        (dict(name__icontains='lisa'), 2),
        (dict(name__istartswith='l'), 3),
        (dict(name__istartswith='l', age__gt=30), 2)
    ]
)
async def test_filter(user_cls, query, count, mock_users):
    ret = await user_cls.objects.filter(**query).count()
    assert ret == count


async def test_first(user_cls, mock_users):
    u = await user_cls.objects.first()
    assert isinstance(u, user_cls)


async def test_filter_only_fields(user_cls):
    u = await user_cls.objects.only('id').first()
    assert isinstance(u, user_cls)
    assert isinstance(u.id, ObjectId)
    assert not u.name
    assert not u.age


async def test_filter_exclude_fields(user_cls):
    u = await user_cls.objects.exclude('id', 'name').first()
    assert isinstance(u, user_cls)
    assert isinstance(u.like, list)
    assert not u.id
    assert not u.name
