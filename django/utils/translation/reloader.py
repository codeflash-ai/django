import gettext
from pathlib import Path

from asgiref.local import Local

from django.apps import apps
from django.utils.autoreload import is_django_module
from django.utils.translation import trans_real


def watch_for_translation_changes(sender, **kwargs):
    """Register file watchers for .mo files in potential locale paths."""
    from django.conf import settings

    if settings.USE_I18N:
        directories = [Path("locale")]
        directories.extend(
            Path(config.path) / "locale"
            for config in apps.get_app_configs()
            if not is_django_module(config.module)
        )
        directories.extend(Path(p) for p in settings.LOCALE_PATHS)
        for path in directories:
            sender.watch_dir(path, "**/*.mo")


def translation_file_changed(sender, file_path, **kwargs):
    """Clear the internal translations cache if a .mo file is modified."""
    if file_path.suffix == ".mo":
        gettext._translations.clear()
        trans_real._translations.clear()
        trans_real._default = None
        # Only re-instantiate Local if necessary to avoid unnecessary allocations
        if not isinstance(trans_real._active, Local):
            trans_real._active = Local()
        else:
            # Clear the Local instance instead of re-instantiating
            # This matches Local's behavior of trans_real._active = Local()
            # Clear .__storage__ and .__ident_func__ attributes
            trans_real._active.__dict__.clear()
        return True
