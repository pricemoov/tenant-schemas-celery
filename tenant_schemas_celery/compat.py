from django_pgschemas.test.cases import TenantTestCase
from django_pgschemas.models import TenantMixin
from django_pgschemas.utils import get_tenant_model

def get_public_schema_name():
    return 'public'