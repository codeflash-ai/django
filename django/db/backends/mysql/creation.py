import os
import subprocess
import sys

from django.db.backends.base.creation import BaseDatabaseCreation

from .client import DatabaseClient


class DatabaseCreation(BaseDatabaseCreation):
    def sql_table_creation_suffix(self):
        # Optimization: Avoid unnecessary list creation and membership checks.
        # Directly build the suffix string using local variables.
        test_settings = self.connection.settings_dict["TEST"]
        charset = test_settings["CHARSET"]
        collation = test_settings["COLLATION"]
        if charset and collation:
            return f"CHARACTER SET {charset} COLLATE {collation}"
        elif charset:
            return f"CHARACTER SET {charset}"
        elif collation:
            return f"COLLATE {collation}"
        else:
            return ""

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        try:
            super()._execute_create_test_db(cursor, parameters, keepdb)
        except Exception as e:
            if len(e.args) < 1 or e.args[0] != 1007:
                # All errors except "database exists" (1007) cancel tests.
                self.log("Got an error creating the test database: %s" % e)
                sys.exit(2)
            else:
                raise

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        # Optimization: Minimize repeated lookups and duplicate work
        settings_dict = self.connection.settings_dict
        ops = self.connection.ops

        source_database_name = settings_dict["NAME"]
        target_settings = self.get_test_db_clone_settings(suffix)
        target_database_name = target_settings["NAME"]

        quoted_target_db_name = ops.quote_name(target_database_name)
        table_suffix = self.sql_table_creation_suffix()
        test_db_params = {
            "dbname": quoted_target_db_name,
            "suffix": table_suffix,
        }
        with self._nodb_cursor() as cursor:
            try:
                self._execute_create_test_db(cursor, test_db_params, keepdb)
            except Exception:
                if keepdb:
                    return
                try:
                    if verbosity >= 1:
                        self.log(
                            "Destroying old test database for alias %s..."
                            % (
                                self._get_database_display_str(
                                    verbosity, target_database_name
                                ),
                            )
                        )
                    cursor.execute("DROP DATABASE %(dbname)s" % test_db_params)
                    self._execute_create_test_db(cursor, test_db_params, keepdb)
                except Exception as e:
                    self.log("Got an error recreating the test database: %s" % e)
                    sys.exit(2)
        self._clone_db(source_database_name, target_database_name)

    def _clone_db(self, source_database_name, target_database_name):
        # Optimization: Avoid dict merging (dicts are small, but faster in-line)
        cmd_args, cmd_env = DatabaseClient.settings_to_cmd_args_env(
            self.connection.settings_dict, []
        )

        # Only build env dicts if needed
        if cmd_env is not None:
            base_env = os.environ.copy()
            dump_env = load_env = base_env
            dump_env.update(cmd_env)
        else:
            dump_env = load_env = None

        dump_cmd = (
            ["mysqldump"]
            + cmd_args[1:-1]
            + ["--routines", "--events", source_database_name]
        )
        load_cmd = cmd_args[:]
        load_cmd[-1] = target_database_name

        with subprocess.Popen(
            dump_cmd, stdout=subprocess.PIPE, env=dump_env
        ) as dump_proc:
            with subprocess.Popen(
                load_cmd,
                stdin=dump_proc.stdout,
                stdout=subprocess.DEVNULL,
                env=load_env,
            ):
                # Allow dump_proc to receive a SIGPIPE if the load process exits.
                dump_proc.stdout.close()
