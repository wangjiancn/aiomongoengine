import sys
from typing import Dict
from typing import List
from typing import NoReturn
from typing import TYPE_CHECKING
from typing import Union

from motor.motor_asyncio import AsyncIOMotorClient

from .database import Database
from .errors import ConnectionError

if TYPE_CHECKING:
    from .document import Document

DEFAULT_CONNECTION_NAME = 'default'

registered_collections: Dict[str, 'Document'] = {}
_connection_settings: Dict[str, Dict[str, Union[str, int]]] = {}
_connections: Dict[str, 'AsyncIOMotorClient'] = {}
_default_dbs: Dict[str, 'AsyncIOMotorClient'] = {}


def get_collections() -> Dict[str, 'Document']:
    """ Return all registered document as Dict[class_name,'Document']. """
    return registered_collections


def get_collection_list() -> List['Document']:
    """ Return all registered document as List. """

    collection_list = list(set(registered_collections.values()))
    return collection_list


def register_connection(db, alias, **kwargs):
    global _connection_settings
    global _default_dbs

    _connection_settings[alias] = kwargs
    _default_dbs[alias] = db


def cleanup():
    global _connections
    global _connection_settings
    global _default_dbs

    _connections = {}
    _connection_settings = {}
    _default_dbs = {}


def disconnect(alias: str = DEFAULT_CONNECTION_NAME,
               with_register_document: bool = False) -> NoReturn:
    global _connections
    global _connection_settings
    global _default_dbs

    if alias in _connections:
        connection = _connections[alias]
        if with_register_document:
            for document in registered_collections.values():
                document.close(connection)
        connection.close()
        del _connections[alias]
        del _connection_settings[alias]
        del _default_dbs[alias]


def get_connection(alias: str = DEFAULT_CONNECTION_NAME,
                   db: str = None) -> Database:
    global _connections
    global _default_dbs

    if alias not in _connections:
        conn_settings = _connection_settings[alias].copy()
        db = conn_settings.pop('name', None)

        connection_class = AsyncIOMotorClient
        if 'replicaSet' in conn_settings:
            # Discard port since it can't be used on MongoReplicaSetClient
            conn_settings.pop('port', None)

            # Discard replicaSet if not base string
            if not isinstance(conn_settings['replicaSet'], 'str'):
                conn_settings.pop('replicaSet', None)

        try:
            _connections[alias] = connection_class(**conn_settings)
        except Exception:
            exc_info = sys.exc_info()
            err = ConnectionError(
                "Cannot connect to database %s :\n%s" % (alias, exc_info[1]))
            raise err

    try:
        if not _connections[alias].connected:
            _connections[alias].open_sync()
    except Exception:
        exc_info = sys.exc_info()
        err = ConnectionError(
            "Cannot connect to database %s :\n%s" % (alias, exc_info[1]))
        raise err

    if db is None:
        database = getattr(_connections[alias], _default_dbs[alias])
    else:
        database = getattr(_connections[alias], db)
    return Database(_connections[alias], database)


def connect(db: str,
            alias: str = DEFAULT_CONNECTION_NAME,
            **kwargs) -> 'Database':
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases.  Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

    Extra keyword-arguments are passed to Motor when connecting to the database.
    """
    global _connections
    if alias not in _connections:
        kwargs['name'] = db
        register_connection(db, alias, **kwargs)

    return get_connection(alias, db=db)
