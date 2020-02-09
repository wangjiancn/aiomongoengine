import asyncio
from typing import Type

import pytest
from aiomongoengine import connect
from aiomongoengine import Document
from aiomongoengine import fields


class TestDoc(Document):
    name = fields.StringField(default='', unique=True)
    age = fields.IntField()
    like = fields.ListField(fields.StringField())
    order = fields.IntField()


@pytest.fixture(scope='session')
def user_cls() -> Type[TestDoc]:
    return TestDoc


@pytest.fixture(scope='session')
async def mock_users(user_cls):
    await user_cls.drop_collection()
    users = [
        user_cls(order=1, name='Lisa Bruce', age=10, like=['swim', 'run']),
        user_cls(order=1, name='Michael Adams', age=14, like=['swim', 'run']),
        user_cls(order=1, name='Christina Parsons', age=22, like=['swim']),
        user_cls(order=2, name='Christopher Smith', age=24, like=['run']),
        user_cls(order=2, name='Cynthia Stevens', age=28, like=['swim', 'run']),
        user_cls(order=2, name='Jason Smith', age=32, like=['swim', 'run']),
        user_cls(order=3, name='Lindsay Thompson', age=34, like=['swim']),
        user_cls(order=3, name='Lisa Brown', age=38, like=['swim', 'run']),
        user_cls(order=3, name='Stephanie Flores', age=44)
    ]
    await user_cls.objects.insert(users)
    return users


@pytest.fixture(scope="session")
def event_loop():
    """ Run coroutines in non async code by using a new event_loop """
    old_loop = asyncio.get_event_loop()
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        yield new_loop
    finally:
        asyncio.set_event_loop(old_loop)


@pytest.fixture(autouse=True, scope='session')
def connect_mongo(event_loop):
    db = connect('test',
                 port=20000,
                 username='user',
                 password='123456',
                 authSource='health')
    return db
