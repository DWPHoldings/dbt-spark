from .udf import UDFRegistry

from uuid import uuid4


@UDFRegistry.udf('uuid_string')
def uuid_string():
    return uuid4()


def test_uuid_string():
    id = uuid_string()
    assert id is not None
    print(f'uuid_string -> {id}')
    assert 'uuid_string' in UDFRegistry.registry
