from .udf import UDFRegistry

from uuid import uuid4


@UDFRegistry.udf('test_udf')
def test_udf(one: str, two: str):
    return f'{one}-{two}'


@UDFRegistry.udf('uuid_string')
def uuid_string():
    return uuid4()
