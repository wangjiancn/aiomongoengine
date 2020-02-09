import pytest
from aiomongoengine import get_collection_list
from aiomongoengine import get_collections


def test_get_collection_list(user_cls):
    collection_list = get_collection_list()
    assert user_cls in collection_list


def test_get_collections(user_cls):
    collections = get_collections()
    assert user_cls.__collection__ in collections


@pytest.mark.asyncio
async def test_document_crud(user_cls):
    user = user_cls(name='test', age=22, like=['book'])

    await user.save()
    assert await user_cls.objects.filter(id=user.id).exists()

    user.name = 'test_change'
    await user.save()
    await user.reload()
    assert user.name == 'test_change'

    await user.update(name='test')
    await user.reload()
    assert user.name == 'test'

    await user.delete()
    assert not await user_cls.objects.filter(id=user.id).exists()


@pytest.mark.asyncio
async def test_create_indexes(user_cls):
    ret = await user_cls.ensure_index()
    return 'name_1' in ret
