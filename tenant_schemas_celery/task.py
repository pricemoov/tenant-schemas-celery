import celery
from celery.app.task import Task
from django_pgschemas import get_current_schema

from tenant_schemas_celery.cache import SimpleCache

_shared_storage = {}


class SharedTenantCache(SimpleCache):
    def __init__(self):
        super(SharedTenantCache, self).__init__(storage=_shared_storage)


class TenantTask(Task):
    """Custom Task class that injects db schema currently used to the task's
    keywords so that the worker can use the same schema.
    """

    abstract = True

    tenant_cache_seconds = None

    @classmethod
    def tenant_cache(cls):
        return SharedTenantCache()

    def _update_headers(self, kw):
        headers = kw.setdefault("headers", {})
        headers.setdefault("_schema_name", get_current_schema().schema_name)

    def apply(self, args=None, kwargs=None, *arg, **kw):
        self._update_headers(kw)
        return super(TenantTask, self).apply(args, kwargs, *arg, **kw)
