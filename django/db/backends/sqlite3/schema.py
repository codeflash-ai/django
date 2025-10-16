import copy
from decimal import Decimal

from django.apps.registry import Apps
from django.db import NotSupportedError
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.ddl_references import Statement
from django.db.backends.utils import strip_quotes
from django.db.models import NOT_PROVIDED, UniqueConstraint


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_delete_table = "DROP TABLE %(table)s"
    sql_create_fk = None
    sql_create_inline_fk = (
        "REFERENCES %(to_table)s (%(to_column)s) DEFERRABLE INITIALLY DEFERRED"
    )
    sql_create_column_inline_fk = sql_create_inline_fk
    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_create_unique = "CREATE UNIQUE INDEX %(name)s ON %(table)s (%(columns)s)"
    sql_delete_unique = "DROP INDEX %(name)s"
    sql_alter_table_comment = None
    sql_alter_column_comment = None

    def __enter__(self):
        # Some SQLite schema alterations need foreign key constraints to be
        # disabled. Enforce it here for the duration of the schema edition.
        if not self.connection.disable_constraint_checking():
            raise NotSupportedError(
                "SQLite schema editor cannot be used while foreign key "
                "constraint checks are enabled. Make sure to disable them "
                "before entering a transaction.atomic() context because "
                "SQLite does not support disabling them in the middle of "
                "a multi-statement transaction."
            )
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.check_constraints()
        super().__exit__(exc_type, exc_value, traceback)
        self.connection.enable_constraint_checking()

    def quote_value(self, value):
        # The backend "mostly works" without this function and there are use
        # cases for compiling Python without the sqlite3 libraries (e.g.
        # security hardening).
        try:
            import sqlite3

            value = sqlite3.adapt(value)
        except ImportError:
            pass
        except sqlite3.ProgrammingError:
            pass
        # Manual emulation of SQLite parameter quoting
        if isinstance(value, bool):
            return str(int(value))
        elif isinstance(value, (Decimal, float, int)):
            return str(value)
        elif isinstance(value, str):
            return "'%s'" % value.replace("'", "''")
        elif value is None:
            return "NULL"
        elif isinstance(value, (bytes, bytearray, memoryview)):
            # Bytes are only allowed for BLOB fields, encoded as string
            # literals containing hexadecimal data and preceded by a single "X"
            # character.
            return "X'%s'" % value.hex()
        else:
            raise ValueError(
                "Cannot quote parameter value %r of type %s" % (value, type(value))
            )

    def prepare_default(self, value):
        return self.quote_value(value)

    def _remake_table(
        self, model, create_field=None, delete_field=None, alter_fields=None
    ):
        """
        Shortcut to transform a model from old_model into new_model

        This follows the correct procedure to perform non-rename or column
        addition operations based on SQLite's documentation

        https://www.sqlite.org/lang_altertable.html#caution

        The essential steps are:
          1. Create a table with the updated definition called "new__app_model"
          2. Copy the data from the existing "app_model" table to the new table
          3. Drop the "app_model" table
          4. Rename the "new__app_model" table to "app_model"
          5. Restore any index of the previous "app_model" table.
        """

        # Self-referential fields must be recreated rather than copied from
        # the old model to ensure their remote_field.field_name doesn't refer
        # to an altered field.
        def is_self_referential(f):
            return f.is_relation and f.remote_field.model is model

        local_concrete_fields = model._meta.local_concrete_fields
        body = {
            f.name: f.clone() if is_self_referential(f) else f
            for f in local_concrete_fields
        }

        # Prepare mapping in a single pass to minimize attribute lookups.
        mapping = {}
        for f in local_concrete_fields:
            # Use is False to avoid match for None, allows generated to be None or False explicitly
            if f.generated is False:
                mapping[f.column] = self.quote_name(f.column)

        rename_mapping = {}
        restore_pk_field = None
        if alter_fields is None:
            alter_fields = []
        # `any(getattr(...))` can be optimized by using a generator variable.
        create_field_pk = getattr(create_field, "primary_key", False)
        alter_fields_pk = False
        for _, new_field in alter_fields:
            if getattr(new_field, "primary_key", False):
                alter_fields_pk = True
                break
        if create_field_pk or alter_fields_pk:
            # Avoid repeated `any`, optimize with set for `name == new_field.name`
            # in the old PK detection below.
            alter_new_names = {new_field.name for _, new_field in alter_fields}
            for name, field in list(body.items()):
                if field.primary_key and name not in alter_new_names:
                    field.primary_key = False
                    restore_pk_field = field
                    if field.auto_created:
                        del body[name]
                        del mapping[field.column]

        # Add in any created fields
        if create_field:
            body[create_field.name] = create_field
            if (
                create_field.db_default is NOT_PROVIDED
                and not (create_field.many_to_many or create_field.generated)
                and create_field.concrete
            ):
                mapping[create_field.column] = self.prepare_default(
                    self.effective_default(create_field)
                )

        # Add in any altered fields, skipping unnecessary .pop lookups if key not present
        for old_field, new_field in alter_fields:
            body.pop(old_field.name, None)
            mapping.pop(old_field.column, None)
            body[new_field.name] = new_field
            if old_field.null and not new_field.null:
                if new_field.db_default is NOT_PROVIDED:
                    default = self.prepare_default(self.effective_default(new_field))
                else:
                    default, _ = self.db_default_sql(new_field)
                case_sql = "coalesce(%(col)s, %(default)s)" % {
                    "col": self.quote_name(old_field.column),
                    "default": default,
                }
                mapping[new_field.column] = case_sql
            else:
                mapping[new_field.column] = self.quote_name(old_field.column)
            rename_mapping[old_field.name] = new_field.name

        # Remove any deleted fields
        if delete_field:
            del body[delete_field.name]
            mapping.pop(delete_field.column, None)
            if (
                delete_field.many_to_many
                and delete_field.remote_field.through._meta.auto_created
            ):
                return self.delete_model(delete_field.remote_field.through)

        # Work inside a new app registry
        apps = Apps()

        # Work out the new value of unique_together, taking renames into
        # account. Use a list comprehension to minimize iterations.
        unique_together = [
            [rename_mapping.get(n, n) for n in unique]
            for unique in model._meta.unique_together
        ]
        indexes = model._meta.indexes
        if delete_field:
            delete_name = delete_field.name
            # Use a generator for index-fields lookups (index.fields is usually short)
            indexes = [index for index in indexes if delete_name not in index.fields]
        constraints = list(model._meta.constraints)

        # Copy only if needed
        body_copy = copy.deepcopy(body)

        meta_contents = {
            "app_label": model._meta.app_label,
            "db_table": model._meta.db_table,
            "unique_together": unique_together,
            "indexes": indexes,
            "constraints": constraints,
            "apps": apps,
        }
        meta = type("Meta", (), meta_contents)
        body_copy["Meta"] = meta
        body_copy["__module__"] = model.__module__
        type(model._meta.object_name, model.__bases__, body_copy)

        # Construct model for the new table - use deepcopy as above
        body_copy_2 = copy.deepcopy(body)
        meta_contents_2 = {
            "app_label": model._meta.app_label,
            "db_table": "new__%s" % strip_quotes(model._meta.db_table),
            "unique_together": unique_together,
            "indexes": indexes,
            "constraints": constraints,
            "apps": apps,
        }
        meta2 = type("Meta", (), meta_contents_2)
        body_copy_2["Meta"] = meta2
        body_copy_2["__module__"] = model.__module__
        new_model = type(
            "New%s" % model._meta.object_name, model.__bases__, body_copy_2
        )

        # Remove the automatically recreated default primary key, if it has
        # been deleted.
        if (
            delete_field
            and getattr(delete_field, "attname", None)
            == getattr(new_model._meta.pk, "attname", None)
            and hasattr(new_model._meta, "pk")
            and new_model._meta.pk is not None
        ):
            auto_pk = new_model._meta.pk
            if hasattr(new_model, auto_pk.attname):
                delattr(new_model, auto_pk.attname)
            # Defensive: remove field if present
            if auto_pk in new_model._meta.local_fields:
                new_model._meta.local_fields.remove(auto_pk)
            # Defensive: set pk to None as in original code
            new_model.pk = None

        # Create a new table with the updated schema.
        self.create_model(new_model)

        # Copy data from the old table into the new table, optimizing for column extraction
        mapping_keys = tuple(mapping)
        quote = self.quote_name
        columns_str = ", ".join(map(quote, mapping_keys))
        values_str = ", ".join(mapping.values())
        self.execute(
            f"INSERT INTO {quote(new_model._meta.db_table)} ({columns_str}) SELECT {values_str} FROM {quote(model._meta.db_table)}"
        )

        # Delete the old table to make way for the new
        self.delete_model(model, handle_autom2m=False)

        # Rename the new table to take way for the old
        self.alter_db_table(
            new_model,
            new_model._meta.db_table,
            model._meta.db_table,
        )

        # Run deferred SQL on correct table in one pass for performance
        if self.deferred_sql:
            for sql in self.deferred_sql:
                self.execute(sql)
            self.deferred_sql = []
        # Fix any PK-removed field
        if restore_pk_field:
            restore_pk_field.primary_key = True

    def delete_model(self, model, handle_autom2m=True):
        if handle_autom2m:
            super().delete_model(model)
        else:
            # Delete the table (and only that)
            self.execute(
                self.sql_delete_table
                % {
                    "table": self.quote_name(model._meta.db_table),
                }
            )
            # Remove all deferred statements referencing the deleted table.
            # Batch build list to avoid removing-while-iterating inefficiency
            to_remove = []
            for sql in self.deferred_sql:
                if isinstance(sql, Statement) and sql.references_table(
                    model._meta.db_table
                ):
                    to_remove.append(sql)
            if to_remove:
                for sql in to_remove:
                    self.deferred_sql.remove(sql)

    def add_field(self, model, field):
        """Create a field on a model."""
        from django.db.models.expressions import Value

        # Special-case implicit M2M tables.
        if field.many_to_many and field.remote_field.through._meta.auto_created:
            self.create_model(field.remote_field.through)
        elif (
            # Primary keys and unique fields are not supported in ALTER TABLE
            # ADD COLUMN.
            field.primary_key
            or field.unique
            or not field.null
            # Fields with default values cannot by handled by ALTER TABLE ADD
            # COLUMN statement because DROP DEFAULT is not supported in
            # ALTER TABLE.
            or self.effective_default(field) is not None
            # Fields with non-constant defaults cannot by handled by ALTER
            # TABLE ADD COLUMN statement.
            or (
                field.db_default is not NOT_PROVIDED
                and not isinstance(field.db_default, Value)
            )
        ):
            self._remake_table(model, create_field=field)
        else:
            super().add_field(model, field)

    def remove_field(self, model, field):
        """
        Remove a field from a model. Usually involves deleting a column,
        but for M2Ms may involve deleting a table.
        """
        # M2M fields are a special case
        if field.many_to_many:
            # For implicit M2M tables, delete the auto-created table
            if field.remote_field.through._meta.auto_created:
                self.delete_model(field.remote_field.through)
            # For explicit "through" M2M fields, do nothing
        elif (
            self.connection.features.can_alter_table_drop_column
            # Primary keys, unique fields, indexed fields, and foreign keys are
            # not supported in ALTER TABLE DROP COLUMN.
            and not field.primary_key
            and not field.unique
            and not field.db_index
            and not (field.remote_field and field.db_constraint)
        ):
            super().remove_field(model, field)
        # For everything else, remake.
        else:
            # It might not actually have a column behind it
            if field.db_parameters(connection=self.connection)["type"] is None:
                return
            self._remake_table(model, delete_field=field)

    def _alter_field(
        self,
        model,
        old_field,
        new_field,
        old_type,
        new_type,
        old_db_params,
        new_db_params,
        strict=False,
    ):
        """Perform a "physical" (non-ManyToMany) field update."""
        # Use "ALTER TABLE ... RENAME COLUMN" if only the column name
        # changed and there aren't any constraints.
        if (
            old_field.column != new_field.column
            and self.column_sql(model, old_field) == self.column_sql(model, new_field)
            and not (
                old_field.remote_field
                and old_field.db_constraint
                or new_field.remote_field
                and new_field.db_constraint
            )
        ):
            return self.execute(
                self._rename_field_sql(
                    model._meta.db_table, old_field, new_field, new_type
                )
            )
        # Alter by remaking table
        self._remake_table(model, alter_fields=[(old_field, new_field)])
        # Rebuild tables with FKs pointing to this field.
        old_collation = old_db_params.get("collation")
        new_collation = new_db_params.get("collation")
        if new_field.unique and (
            old_type != new_type or old_collation != new_collation
        ):
            related_models = set()
            opts = new_field.model._meta
            for remote_field in opts.related_objects:
                # Ignore self-relationship since the table was already rebuilt.
                if remote_field.related_model == model:
                    continue
                if not remote_field.many_to_many:
                    if remote_field.field_name == new_field.name:
                        related_models.add(remote_field.related_model)
                elif new_field.primary_key and remote_field.through._meta.auto_created:
                    related_models.add(remote_field.through)
            if new_field.primary_key:
                for many_to_many in opts.many_to_many:
                    # Ignore self-relationship since the table was already rebuilt.
                    if many_to_many.related_model == model:
                        continue
                    if many_to_many.remote_field.through._meta.auto_created:
                        related_models.add(many_to_many.remote_field.through)
            for related_model in related_models:
                self._remake_table(related_model)

    def _alter_many_to_many(self, model, old_field, new_field, strict):
        """Alter M2Ms to repoint their to= endpoints."""
        old_through = old_field.remote_field.through
        new_through = new_field.remote_field.through
        old_through_table = old_through._meta.db_table
        new_through_table = new_through._meta.db_table
        if old_through_table == new_through_table:
            # The field name didn't change, but some options did, so we have to
            # propagate this altering.
            # Save field name objects locally to skip redundant getattr lookups
            old_through_meta = old_through._meta
            new_through_meta = new_through._meta
            self._remake_table(
                old_through,
                alter_fields=[
                    (
                        old_through_meta.get_field(old_field.m2m_reverse_field_name()),
                        new_through_meta.get_field(new_field.m2m_reverse_field_name()),
                    ),
                    (
                        old_through_meta.get_field(old_field.m2m_field_name()),
                        new_through_meta.get_field(new_field.m2m_field_name()),
                    ),
                ],
            )
            return

        # Make a new through table
        self.create_model(new_through)

        # Prepare local names to minimize method calls in the INSERT
        qname = self.quote_name
        new_m2m_col = new_field.m2m_column_name()
        new_m2m_rev = new_field.m2m_reverse_name()
        old_m2m_col = old_field.m2m_column_name()
        old_m2m_rev = old_field.m2m_reverse_name()
        # Compose the SQL insert in a single step
        insert_sql = (
            f"INSERT INTO {qname(new_through_table)} (id, {new_m2m_col}, {new_m2m_rev}) "
            f"SELECT id, {old_m2m_col}, {old_m2m_rev} FROM {qname(old_through_table)}"
        )
        self.execute(insert_sql)

        # Delete the old through table
        self.delete_model(old_through)

    def add_constraint(self, model, constraint):
        if isinstance(constraint, UniqueConstraint) and (
            constraint.condition
            or constraint.contains_expressions
            or constraint.include
            or constraint.deferrable
        ):
            super().add_constraint(model, constraint)
        else:
            self._remake_table(model)

    def remove_constraint(self, model, constraint):
        if isinstance(constraint, UniqueConstraint) and (
            constraint.condition
            or constraint.contains_expressions
            or constraint.include
            or constraint.deferrable
        ):
            super().remove_constraint(model, constraint)
        else:
            self._remake_table(model)

    def _collate_sql(self, collation):
        return "COLLATE " + collation
