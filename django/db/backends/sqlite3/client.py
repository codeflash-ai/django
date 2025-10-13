from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "sqlite3"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        # Avoids a potentially costly list concatenation for large parameters by preallocating and assigning directly.
        args = [None] * (2 + len(parameters))
        args[0] = cls.executable_name
        args[1] = settings_dict["NAME"]
        if parameters:
            args[2:] = parameters
        return args, None
