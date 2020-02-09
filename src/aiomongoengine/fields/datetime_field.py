from datetime import datetime
from typing import Union

import arrow

from .base_field import BaseField

FORMAT = "%Y-%m-%dT%H:%M:%S%z"
TIMEZONE = 'Asia/Shanghai'


class DateTimeField(BaseField):
    """ Field responsible for storing dates.


    * `auto_now_on_insert` - When an instance is created sets the field to datetime.now()
    * `auto_now_on_update` - Whenever the instance is saved the field value gets updated to datetime.now()
    * `tz` - Defines the timezone used for auto_now_on_insert and auto_now_on_update and should be enforced on all values of this datetime field. To interpret all times as UTC use tz=datetime.timezone.utc (Defaults: to None, which means waht you put in comes out again)
    """

    def __init__(self,
                 auto_now_on_insert: bool = False,
                 auto_now_on_update: bool = False,
                 tz=None,
                 *args,
                 **kw):
        """ Field responsible for storing dates.

        :param auto_now_on_insert:(bool) When an instance is created sets the
            field to now.
        :param auto_now_on_update:(bool) Whenever the instance is saved the
            field value gets updated to now.
        :param tz:(str) timezone
        """
        super(DateTimeField, self).__init__(*args, **kw)
        self.auto_now_on_insert = auto_now_on_insert
        self.auto_now_on_update = auto_now_on_update
        self.tz = tz

    def get_db_prep_value(self, value) -> Union[None, datetime]:
        if self.auto_now_on_insert and value is None:
            return arrow.now(tz=self.tz).datetime

        if self.auto_now_on_update:
            return arrow.now(tz=self.tz).datetime

        if not isinstance(value, datetime):
            if self.tz:
                return arrow.get(value, self.tz).datetime
            else:
                return arrow.get(value).datetime

        return value

    def get_value(self, value) -> datetime:
        value = super().get_value(value)
        if value is None:
            return None
        return arrow.get(value).datetime

    def to_son(self, value):
        if value is None:
            return None
        return arrow.get(value).datetime

    def from_son(self, value):
        return self.to_son(value)

    def validate(self, value):
        return value is None or isinstance(value, datetime)
