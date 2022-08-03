from django_pgschemas.test.cases import TenantTestCase
from django_pgschemas.models import TenantMixin
from django_pgschemas.utils import get_tenant_model

def get_public_schema_name():
    return 'public'


class CompatibleConnection:
    def __init__(self, connection: Any):
        self.connection = connection

    @staticmethod
    def get_public_schema_name():
        return get_public_schema_name()

    def get_schema(self):
        if self._is_new():
            return self.connection._schema
        return self.connection.schema

    def set_schema_to_public(self):
        if self._is_new():
            self.connection._set_schema_to_public()
        else:
            self.connection.set_schema_to_public()

    def set_schema(self: Any, schema: Any, *args, **kwargs) -> None:
        if self._is_new():
            self.connection._set_schema(schema, *args, **kwargs)
        else:
            self.connection.set_schema(schema, *args, **kwargs)

    def _is_new(self) -> bool:
        return not hasattr(self.connection, "schema")

