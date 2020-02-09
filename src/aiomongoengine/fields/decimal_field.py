from decimal import Decimal
from decimal import InvalidOperation
from decimal import ROUND_HALF_UP
from typing import Union

from .base_field import BaseField


class DecimalField(BaseField):
    """ Field responsible for storing fixed-point decimal numbers
    (:py:class:`decimal.Decimal`). """

    def __init__(self,
                 min_value: Union[int, float] = None,
                 max_value: Union[int, float] = None,
                 force_string: bool = False,
                 precision=2,
                 rounding=ROUND_HALF_UP,
                 *args, **kw):
        """
        :param min_value: Raises a validation error if the decimal being
            stored is lesser than this value
        :param max_value: Raises a validation error if the decimal being
            stored is greater than this value
        :param force_string: Force convert to string when save
        :param precision: Number of decimal places to store.
        :param rounding: The rounding rule from the python decimal library:
            * decimal.ROUND_CEILING (towards Infinity)
            * decimal.ROUND_DOWN (towards zero)
            * decimal.ROUND_FLOOR (towards -Infinity)
            * decimal.ROUND_HALF_DOWN (to nearest with ties going towards zero)
            * decimal.ROUND_HALF_EVEN (to nearest with ties going to nearest
                even integer)
            * decimal.ROUND_HALF_UP (to nearest with ties going away from zero)
            * decimal.ROUND_UP (away from zero)
            * decimal.ROUND_05UP (away from zero if last digit after rounding
                towards zero would have been 0 or 5; otherwise towards zero)
        """
        super(DecimalField, self).__init__(*args, **kw)
        self.min_value = Decimal(min_value) if min_value else None
        self.max_value = Decimal(max_value) if min_value else None
        self.precision = Decimal(".%s" % ("0" * precision))
        self.rounding = rounding
        self.force_string = force_string

    def to_son(self, value):
        if value is None:
            return value
        if self.force_string:
            return str(self.from_son(value))
        return float(self.from_son(value))

    def from_son(self, value):
        if value is None:
            return value

        # Convert to string for python 2.6 before casting to Decimal
        try:
            value = Decimal("%s" % value)
        except (TypeError, ValueError, InvalidOperation):
            return value
        return value.quantize(
            Decimal(".%s" % ("0" * self.precision)), rounding=self.rounding
        )

    def validate(self, value):
        if not isinstance(value, Decimal):
            if not isinstance(value, str):
                value = str(value)
            try:
                value = Decimal(value)
            except (TypeError, ValueError, InvalidOperation) as exc:
                self.error("Could not convert value to decimal: %s" % exc)

        if self.min_value is not None and value < self.min_value:
            self.error("Decimal value is too small")

        if self.max_value is not None and value > self.max_value:
            self.error("Decimal value is too large")
