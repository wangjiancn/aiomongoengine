from aiomongoengine import Q
from aiomongoengine import QNot


def test_q(user_cls):
    q = ~Q(name__ne=1)
    o = q.to_query(user_cls)
    assert 'name' in o


def test_op(user_cls):
    q = Q(age__gt=10, name__iexact='mei') | \
        (Q(age__lt=5) | (Q(name='w') & Q(name='j'))) & \
        ~Q(age=1)
    o = q.to_query(user_cls)
    assert isinstance(o, dict)


def test_q_not(user_cls):
    q = QNot(Q(a=1, b=2))
    q = q.to_query(user_cls)
    print(q)
