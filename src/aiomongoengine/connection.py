from typing import Dict
from typing import List
from typing import TYPE_CHECKING
from typing import Union

from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReadPreference
from pymongo import uri_parser
from pymongo.database import _check_name

if TYPE_CHECKING:
    from .document import Document

DEFAULT_CONNECTION_NAME = "default"
DEFAULT_DATABASE_NAME = "test"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 27017

registered_collections = {}  # type: Dict[str, 'Document']
_connection_settings = {}  # type: Dict[str, Dict[str, Union[str, int]]] 
_connections = {}  # type: Dict[str, 'AsyncIOMotorClient']
_dbs = {}  # type:  Dict[str, 'AsyncIOMotorClient']

READ_PREFERENCE = ReadPreference.PRIMARY


class ConnectionFailure(Exception):
    """Error raised when the database connection can't be established or
    when a connection with a requested alias can't be retrieved.
    """
    pass


def get_collections() -> Dict[str, 'Document']:
    """ Return all registered document as Dict[class_name,'Document']. """
    return registered_collections


def get_document(name: str) -> 'Document':
    return get_collections().get(name)


def get_collection_list() -> List['Document']:
    """ Return all registered document as List. """

    collection_list = list(set(registered_collections.values()))
    return collection_list


def _check_db_name(name):
    """Check if a database name is valid.
    This functionality is copied from pymongo Database class constructor.
    """
    if not isinstance(name, str):
        raise TypeError("name must be an instance of %s" % str)
    elif name != "$external":
        _check_name(name)


def _get_connection_settings(
        db=None,
        name=None,
        host=None,
        port=None,
        read_preference=READ_PREFERENCE,
        username=None,
        password=None,
        authentication_source=None,
        authentication_mechanism=None,
        **kwargs
) -> 'dict':
    """Get the connection settings as a dict

    : param db: the name of the database to use, for compatibility with connect
    : param name: the name of the specific database to use
    : param host: the host name of the: program: `mongod` instance to connect to
    : param port: the port that the: program: `mongod` instance is running on
    : param read_preference: The read preference for the collection
    : param username: username to authenticate with
    : param password: password to authenticate with
    : param authentication_source: database to authenticate against
    : param authentication_mechanism: database authentication mechanisms.
        By default, use SCRAM-SHA-1 with MongoDB 3.0 and later,
        MONGODB-CR (MongoDB Challenge Response protocol) for older servers.
    : param kwargs: ad-hoc parameters to be passed into the pymongo driver,
        for example maxpoolsize, tz_aware, etc. See the documentation
        for pymongo's `MongoClient` for a full list.
    """
    conn_settings = {
        "name": name or db or DEFAULT_DATABASE_NAME,
        "host": host or DEFAULT_HOST,
        "port": port or DEFAULT_PORT,
        "read_preference": read_preference,
        "username": username,
        "password": password,
        "authentication_source": authentication_source,
        "authentication_mechanism": authentication_mechanism,
    }

    _check_db_name(conn_settings["name"])
    conn_host = conn_settings["host"]

    # Host can be a list or a string, so if string, force to a list.
    if isinstance(conn_host, str):
        conn_host = [conn_host]

    resolved_hosts = []
    for entity in conn_host:

        # Handle URI style connections, only updating connection params which
        # were explicitly specified in the URI.
        if "://" in entity:
            uri_dict = uri_parser.parse_uri(entity)
            resolved_hosts.append(entity)

            database = uri_dict.get("database")
            if database:
                conn_settings["name"] = database

            for param in ("read_preference", "username", "password"):
                if uri_dict.get(param):
                    conn_settings[param] = uri_dict[param]

            uri_options = uri_dict["options"]
            if "replicaset" in uri_options:
                conn_settings["replicaSet"] = uri_options["replicaset"]
            if "authsource" in uri_options:
                conn_settings["authentication_source"] = uri_options["authsource"]
            if "authmechanism" in uri_options:
                conn_settings["authentication_mechanism"] = uri_options["authmechanism"]
            if "readpreference" in uri_options:
                read_preferences = (
                    ReadPreference.NEAREST,
                    ReadPreference.PRIMARY,
                    ReadPreference.PRIMARY_PREFERRED,
                    ReadPreference.SECONDARY,
                    ReadPreference.SECONDARY_PREFERRED,
                )

                # Starting with PyMongo v3.5, the "readpreference" option is
                # returned as a string (e.g. "secondaryPreferred") and not an
                # int (e.g. 3).
                # TODO simplify the code below once we drop support for
                # PyMongo v3.4.
                read_pf_mode = uri_options["readpreference"]
                if isinstance(read_pf_mode, str):
                    read_pf_mode = read_pf_mode.lower()
                for preference in read_preferences:
                    if (
                            preference.name.lower() == read_pf_mode
                            or preference.mode == read_pf_mode
                    ):
                        conn_settings["read_preference"] = preference
                        break
        else:
            resolved_hosts.append(entity)
    conn_settings["host"] = resolved_hosts

    # Deprecated parameters that should not be passed on
    kwargs.pop("slaves", None)
    kwargs.pop("is_slave", None)

    conn_settings.update(kwargs)
    return conn_settings


def register_connection(
        alias,
        db=None,
        name=None,
        host=None,
        port=None,
        read_preference=READ_PREFERENCE,
        username=None,
        password=None,
        authentication_source=None,
        authentication_mechanism=None,
        **kwargs
):
    """Register the connection settings.

    : param alias: the name that will be used to refer to this connection throughout MongoEngine
    : param db: the name of the database to use, for compatibility with connect
    : param name: the name of the specific database to use
    : param host: the host name of the: program: `mongod` instance to connect to
    : param port: the port that the: program: `mongod` instance is running on
    : param read_preference: The read preference for the collection
    : param username: username to authenticate with
    : param password: password to authenticate with
    : param authentication_source: database to authenticate against
    : param authentication_mechanism: database authentication mechanisms.
        By default, use SCRAM-SHA-1 with MongoDB 3.0 and later,
        MONGODB-CR (MongoDB Challenge Response protocol) for older servers.
    : param kwargs: ad-hoc parameters to be passed into the pymongo driver,
        for example maxpoolsize, tz_aware, etc. See the documentation
        for pymongo's `MongoClient` for a full list.
    """
    conn_settings = _get_connection_settings(
        db=db,
        name=name,
        host=host,
        port=port,
        read_preference=kwargs.pop('readPreference', read_preference),
        username=username,
        password=password,
        authentication_source=kwargs.pop('authSource', authentication_source),
        authentication_mechanism=kwargs.pop('authMechanism', authentication_mechanism),
        **kwargs
    )
    _connection_settings[alias] = conn_settings


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    """Close the connection with a given alias."""
    from aiomongoengine.base.common import _get_documents_by_db
    from aiomongoengine import Document

    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]

    if alias in _dbs:
        # Detach all cached collections in Documents
        for doc_cls in _get_documents_by_db(alias, DEFAULT_CONNECTION_NAME):
            if issubclass(doc_cls, Document):  # Skip EmbeddedDocument
                doc_cls._disconnect()

        del _dbs[alias]

    if alias in _connection_settings:
        del _connection_settings[alias]


def disconnect_all():
    """Close all registered database."""
    for alias in list(_connections.keys()):
        disconnect(alias)


def get_connection(
        alias: str = DEFAULT_CONNECTION_NAME,
        reconnect: bool = False
) -> 'AsyncIOMotorClient':
    """Return a connection with a given alias."""

    # Connect to the database if not already connected
    if reconnect:
        disconnect(alias)

    # If the requested alias already exists in the _connections list, return
    # it immediately.
    if alias in _connections:
        return _connections[alias]

    # Validate that the requested alias exists in the _connection_settings.
    # Raise ConnectionFailure if it doesn't.
    if alias not in _connection_settings:
        if alias == DEFAULT_CONNECTION_NAME:
            msg = "You have not defined a default connection"
        else:
            msg = 'Connection with alias "%s" has not been defined' % alias
        raise ConnectionFailure(msg)

    conn_settings = _connection_settings[alias].copy()

    try:
        auth_kwargs = {}
        if conn_settings["username"] and (
                conn_settings["password"] or
                conn_settings["authentication_mechanism"] == "MONGODB-X509"):
            auth_kwargs = {
                'authentication_mechanism': conn_settings.pop('authentication_mechanism'),
                'authSource': conn_settings.pop('authentication_source'),
                'username': conn_settings.pop('username'),
                'password': conn_settings.pop('password')
            }
            none_key = [k for k, v in auth_kwargs.items() if v is None]
            for key in none_key:
                del auth_kwargs[key]
        del conn_settings['name']
        connection = AsyncIOMotorClient(**conn_settings, **auth_kwargs)
    except Exception as e:
        raise ConnectionFailure("Cannot connect to database %s :\n%s" % (alias, e))
    _connections[alias] = connection
    return connection


def get_db(
        alias=DEFAULT_CONNECTION_NAME,
        reconnect=False
) -> 'AsyncIOMotorDatabase':
    """Get database."""
    if reconnect:
        disconnect(alias)
    if alias not in _dbs:
        conn_setting = _connection_settings[alias].copy()
        db_name = conn_setting['name']
        db = _connections[alias][db_name]
        _dbs[alias] = db
    return _dbs[alias]


def connect(
        db=None,
        alias=DEFAULT_CONNECTION_NAME,
        **kwargs
) -> 'AsyncIOMotorClient':
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases. Provide a separate
    `alias` to connect to a different instance of: program: `mongod`.

    In order to replace a connection identified by a given alias, you'll
    need to call ``disconnect`` first

    See the docstring for `register_connection` for more details about all
    supported kwargs.
    """
    if alias in _connections:
        prev_conn_setting = _connection_settings[alias]
        new_conn_settings = _get_connection_settings(db, **kwargs)

        if new_conn_settings != prev_conn_setting:
            err_msg = (
                u"A different connection with alias `{}` was already "
                u"registered. Use disconnect() first"
            ).format(alias)
            raise ConnectionFailure(err_msg)
    else:
        register_connection(alias, db, **kwargs)

    return get_connection(alias)
