import signal

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "mysql"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        options = settings_dict["OPTIONS"]
        # Pre-fetch all possibly needed values with a single options lookup
        name = settings_dict["NAME"]
        user = options.get("user", settings_dict["USER"])
        password = options.get(
            "password", options.get("passwd", settings_dict["PASSWORD"])
        )
        host = options.get("host", settings_dict["HOST"])
        port = options.get("port", settings_dict["PORT"])
        defaults_file = options.get("read_default_file")
        charset = options.get("charset")
        ssl_options = options.get("ssl", None)
        database = options.get(
            "database",
            options.get("db", name),
        )

        # For env, only set if password exists, avoid setting otherwise
        env = None

        # Use local list and extend once at the end for better performance
        args = [cls.executable_name]
        append = args.append  # Local binding for faster loop

        if defaults_file:
            append(f"--defaults-file={defaults_file}")
        if user:
            append(f"--user={user}")
        if password:
            env = {"MYSQL_PWD": password}
        if host:
            if "/" in host:
                append(f"--socket={host}")
            else:
                append(f"--host={host}")
        if port:
            append(f"--port={port}")
        if ssl_options:
            server_ca = ssl_options.get("ca")
            if server_ca:
                append(f"--ssl-ca={server_ca}")
            client_cert = ssl_options.get("cert")
            if client_cert:
                append(f"--ssl-cert={client_cert}")
            client_key = ssl_options.get("key")
            if client_key:
                append(f"--ssl-key={client_key}")
        if charset:
            append(f"--default-character-set={charset}")
        if database:
            append(database)

        # Only a single list extend, faster than multiple appends
        args.extend(parameters)
        return args, env

    def runshell(self, parameters):
        sigint_handler = signal.getsignal(signal.SIGINT)
        try:
            # Allow SIGINT to pass to mysql to abort queries.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            super().runshell(parameters)
        finally:
            # Restore the original SIGINT handler.
            signal.signal(signal.SIGINT, sigint_handler)
