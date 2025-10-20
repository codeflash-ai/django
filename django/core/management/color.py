"""
Sets up the terminal color scheme.
"""

import functools
import os
import sys

from django.utils import termcolors

_NOCOLOR_PALETTE = termcolors.NOCOLOR_PALETTE

_PALETTES = termcolors.PALETTES

_ROLE_KEYS = tuple(_PALETTES[_NOCOLOR_PALETTE])

_MAKE_STYLE_CACHE = {}

try:
    import colorama

    # Avoid initializing colorama in non-Windows platforms.
    colorama.just_fix_windows_console()
except (
    AttributeError,  # colorama <= 0.4.6.
    ImportError,  # colorama is not installed.
    # If just_fix_windows_console() accesses sys.stdout with
    # WSGIRestrictedStdout.
    OSError,
):
    HAS_COLORAMA = False
else:
    HAS_COLORAMA = False


def supports_color():
    """
    Return True if the running system's terminal supports color,
    and False otherwise.
    """

    # Fast path: check minimal needed if definitely not a tty, before further logic.
    stdout = sys.stdout
    if not (hasattr(stdout, "isatty") and stdout.isatty()):
        return False

    # At this point, we know it's a tty, check platform specifics.
    plat = sys.platform
    environ = os.environ

    # Windows specifics.
    if plat == "win32":
        # Fast path: colorama fixed_windows_console handling
        if HAS_COLORAMA and getattr(colorama, "fixed_windows_console", False):
            return True
        # Win terminals known to support VT codes:
        if (
            "ANSICON" in environ
            or "WT_SESSION" in environ
            or environ.get("TERM_PROGRAM") == "vscode"
        ):
            return True
        # Check winreg only if really necessary.
        try:
            import winreg

            try:
                reg_key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Console")
                reg_key_value, _ = winreg.QueryValueEx(reg_key, "VirtualTerminalLevel")
            except FileNotFoundError:
                return False
            else:
                return reg_key_value == 1
        except ImportError:
            return False
    else:
        # Non-windows: user at a tty is enough
        return True


class Style:
    pass


def make_style(config_string=""):
    """
    Create a Style object from the given config_string.

    If config_string is empty django.utils.termcolors.DEFAULT_PALETTE is used.
    """

    # Use a cache for color_settings associated to config_string for fast repeated style setup
    # Do NOT cache Style() itself as it is mutable and roles can be different per config_string
    color_settings = _MAKE_STYLE_CACHE.get(config_string)
    if color_settings is None:
        color_settings = termcolors.parse_color_setting(config_string)
        _MAKE_STYLE_CACHE[config_string] = color_settings

    style = Style()

    # The nocolor palette has all available roles.
    roles = _ROLE_KEYS
    make_style_ = termcolors.make_style

    # Micro-optimization: pre-bind the passthrough function
    def passthrough(x):
        return x

    if color_settings:
        # Optimize by hoisting attribute creation outside loop (-- eliminate closure allocation in hotpath)
        for role in roles:
            format = color_settings.get(role, {})
            if format:
                style_func = make_style_(**format)
            else:
                style_func = passthrough
            setattr(style, role, style_func)
    else:
        # Set all to passthrough in one loop
        for role in roles:
            setattr(style, role, passthrough)

    # For backwards compatibility,
    # set style for ERROR_OUTPUT == ERROR
    style.ERROR_OUTPUT = style.ERROR

    return style


@functools.cache
def no_style():
    """
    Return a Style object with no color scheme.
    """
    return make_style("nocolor")


def color_style(force_color=False):
    """
    Return a Style object from the Django color scheme.
    """
    if not force_color and not supports_color():
        return no_style()
    # Bypass global _MAKE_STYLE_CACHE in make_style for config_string lookup efficiency
    return make_style(os.environ.get("DJANGO_COLORS", ""))
