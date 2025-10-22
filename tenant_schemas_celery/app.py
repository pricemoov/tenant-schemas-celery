from __future__ import absolute_import

from celery import Celery

from django_pgschemas import get_current_schema, activate, SchemaDescriptor
from celery.signals import task_prerun, task_postrun


def get_schema_name_from_task(task):
    # In some cases (like Redis broker) headers are merged with `task.request`.
    task_headers = task.request.headers or task.request
    return task_headers.get("_schema_name")


def switch_schema(task, **kw):
    """Switches schema of the task, before it has been run."""

    old_schema = get_current_schema().schema_name
    setattr(task, "_old_schema", old_schema)

    schema = get_schema_name_from_task(task)

    if old_schema == schema:
        # If the schema has not changed, don't do anything.
        return
    activate(SchemaDescriptor.create(schema))


def restore_schema(task, **kwargs):
    """Switches the schema back to the one from before running the task."""

    try:
        schema_name = getattr(task, "_old_schema")
    except AttributeError:
        return
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
    registry_cls = "tenant_schemas_celery.registry:TenantTaskRegistry"

    def create_task_cls(self):
        return self.subclass_with_self(
            "tenant_schemas_celery.task:TenantTask",
            abstract=True,
            name="TenantTask",
            attribute="_app",
        )

    def _update_headers(self, options: dict):
        headers = options.setdefault("headers", {})

        schema_name = headers.get("_schema_name", get_current_schema().schema_name)
        headers["_schema_name"] = schema_name

        if "headers" not in headers:
            headers["headers"] = {}
        headers["headers"]["_schema_name"] = schema_name

    def send_task(self, name, args=None, kwargs=None, **options):
        self._update_headers(options)
        return super(CeleryApp, self).send_task(
            name, args=args, kwargs=kwargs, **options
        )
