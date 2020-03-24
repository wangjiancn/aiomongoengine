from typing import List
from typing import TYPE_CHECKING
from typing import Union

from aiomongoengine.errors import OperationError
from aiomongoengine.queryset.base import BaseQuerySet
from typing_extensions import TypedDict

if TYPE_CHECKING:
    from ..document import Document


class PaginationDict(TypedDict):
    count: int
    objects: Union[None, List['Document']]
    limit: int
    offset: int
    has_next: bool
    has_previous: bool


class QuerySet(BaseQuerySet):
    """The default queryset, that builds queries and handles a set of results
    returned from a query.

    Wraps a MongoDB cursor, providing :class:`~mongoengine.Document` objects as
    the results.
    """

    _has_more = True
    _len = None
    _result_cache = None

    async def __aiter__(self):
        """Iteration utilises a results cache which iterates the cursor
        in batches of ``ITER_CHUNK_SIZE``.

        If ``self._has_more`` the cursor hasn't been exhausted so cache then
        batch. Otherwise iterate the result_cache.
        """
        self._iter = True

        async for doc in self._cursor:
            yield doc

    def no_cache(self):
        """Convert to a non-caching queryset """
        if self._result_cache is not None:
            raise OperationError("QuerySet already cached")

        return self._clone_into(QuerySetNoCache(self._document, self._collection))

    async def pagination(self,
                         limit: int = 10,
                         offset: int = 0,
                         alias: str = None) -> PaginationDict:

        if not isinstance(limit, int):
            limit = int(limit)
        if not isinstance(offset, int):
            offset = int(offset)

        count = await self.count()
        has_next = count > (limit + offset)
        has_previous = offset > 0
        objects = await self.skip(offset).limit(limit).all(alias=alias)
        return PaginationDict(
            count=count,
            objects=objects,
            limit=limit,
            offset=offset,
            has_next=has_next,
            has_previous=has_previous
        )


class QuerySetNoCache(BaseQuerySet):
    """A non caching QuerySet"""

    def cache(self):
        """Convert to a caching queryset """
        return self._clone_into(QuerySet(self._document, self._collection))

    async def __aiter__(self):
        queryset = self
        if queryset._iter:
            queryset = self.clone()
        queryset.rewind()
        return queryset
