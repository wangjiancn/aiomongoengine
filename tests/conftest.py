import asyncio
from typing import Type

import pytest
from aiomongoengine import connect
from aiomongoengine import Document
from aiomongoengine import fields


class User(Document):
    name = fields.StringField(default='', unique=True)
    age = fields.IntField()
    like = fields.ListField(fields.StringField())


@pytest.fixture(scope='session', autouse=True)
def user_cls() -> Type[Document]:
    return User


@pytest.fixture(scope='session', autouse=True)
async def mock_users(user_cls):
    await user_cls.drop_collection()
    users = [
        user_cls(name='Lisa Bruce', age=10, like=['swim', 'run']),
        user_cls(name='Michael Adams', age=14, like=['swim', 'run']),
        user_cls(name='Christina Parsons', age=22, like=['swim', 'run']),
        user_cls(name='Christopher Smith', age=24, like=['swim', 'run']),
        user_cls(name='Cynthia Stevens', age=28, like=['swim', 'run']),
        user_cls(name='Jason Smith', age=32, like=['swim', 'run']),
        user_cls(name='Lindsay Thompson', age=34, like=['swim', 'run']),
        user_cls(name='Lisa Brown', age=38, like=['swim', 'run']),
        user_cls(name='Stephanie Flores', age=44, like=['swim', 'run'])
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
