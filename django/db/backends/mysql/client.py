import signal

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "mysql"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]
        env = None
        opts = settings_dict["OPTIONS"]

        # Precompute nested dicts to avoid repeated get('ssl', {})
        ssl_opts = opts.get("ssl")
        # Use variable fallback logic only as needed.
        # Flatten option retrievals to avoid redundant .get parsing

        database = opts.get(
            "database",
            opts.get("db", settings_dict["NAME"]),
        )
        user = opts.get("user", settings_dict["USER"])
        passwd = opts.get("passwd", settings_dict["PASSWORD"])
        password = opts.get("password", passwd)
        host = opts.get("host", settings_dict["HOST"])
        port = opts.get("port", settings_dict["PORT"])
        defaults_file = opts.get("read_default_file")
        charset = opts.get("charset")

        # SSL values: avoid creating dict for ssl if not present
        server_ca = ssl_opts.get("ca") if ssl_opts else None
        client_cert = ssl_opts.get("cert") if ssl_opts else None
        client_key = ssl_opts.get("key") if ssl_opts else None

        if defaults_file:
            args.append(f"--defaults-file={defaults_file}")
        if user:
            args.append(f"--user={user}")
        if password:
            env = {"MYSQL_PWD": password}
        if host:
            if "/" in host:
                args.append(f"--socket={host}")
            else:
                args.append(f"--host={host}")
        if port:
            args.append(f"--port={port}")
        if server_ca:
            args.append(f"--ssl-ca={server_ca}")
        if client_cert:
            args.append(f"--ssl-cert={client_cert}")
        if client_key:
            args.append(f"--ssl-key={client_key}")
        if charset:
            args.append(f"--default-character-set={charset}")
        if database:
            args.append(database)
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
