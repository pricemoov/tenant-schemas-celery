from __future__ import absolute_import

try:
    import celery
    from celery import Celery
except ImportError:
    raise ImportError("celery is required to use tenant_schemas_celery")

from django.db import connections
from django_pgschemas import get_current_schema, activate, SchemaDescriptor
from celery.signals import task_prerun, task_postrun


def get_schema_name_from_task(task, kwargs):
    if celery.VERSION[0] < 4:
        # Pop it from the kwargs since tasks don't except the additional kwarg.
        # This change is transparent to the system.
        return kwargs.pop("_schema_name", None)

    # In some cases (like Redis broker) headers are merged with `task.request`.
    task_headers = task.request.headers or task.request
    return task_headers.get("_schema_name")


def switch_schema(task, kwargs, **kw):
    """ Switches schema of the task, before it has been run. """

    old_schema = get_current_schema().schema_name

    setattr(task, "_old_schema", old_schema)
    schema = get_schema_name_from_task(task, kwargs)
    if old_schema == schema:
        # If the schema has not changed, don't do anything.
        return
    activate(SchemaDescriptor.create(schema))



def restore_schema(task, **kwargs):
    """ Switches the schema back to the one from before running the task. """

    if hasattr(task, "_old_schema"):
        schema_name = task._old_schema
    current_schema = get_current_schema()
    if current_schema.schema_name == schema_name:
        # If the schema has not changed, don't do anything.
        return
    activate(SchemaDescriptor.create(schema_name))



task_prerun.connect(
    switch_schema, sender=None, dispatch_uid="tenant_schemas_switch_schema"
)

task_postrun.connect(
    restore_schema, sender=None, dispatch_uid="tenant_schemas_restore_schema"
)


class CeleryApp(Celery):
    registry_cls = 'tenant_schemas_celery.registry:TenantTaskRegistry'
    
    def create_task_cls(self):
        return self.subclass_with_self(
            "tenant_schemas_celery.task:TenantTask",
            abstract=True,
            name="TenantTask",
            attribute="_app",
        )

    def _update_headers(self, kw):
        kw["headers"] = kw.get("headers") or {}
        self._add_current_schema(kw["headers"])

    def _add_current_schema(self, kwds):

        schema_name = kwds.get("_schema_name", get_current_schema().schema_name)
        kwds["_schema_name"] = schema_name
        
        if "headers" not in kwds:
            kwds["headers"] = {}
        kwds["headers"]["_schema_name"] = schema_name

    def send_task(self, name, args=None, kwargs=None, **options):
        if celery.VERSION[0] < 4:
            kwargs = kwargs or {}
            self._add_current_schema(kwargs)

        else:
            # Celery 4.0 introduced strong typing and the `headers` meta dict.
            self._update_headers(options)
        return super(CeleryApp, self).send_task(name, args=args, kwargs=kwargs, **options)
