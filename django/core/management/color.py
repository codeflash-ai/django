"""
Sets up the terminal color scheme.
"""

import functools
import os
import sys

from django.utils import termcolors

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
    HAS_COLORAMA = True


def supports_color():
    """
    Return True if the running system's terminal supports color,
    and False otherwise.
    """

    def vt_codes_enabled_in_windows_registry():
        """
        Check the Windows Registry to see if VT code handling has been enabled
        by default, see https://superuser.com/a/1300251/447564.
        """
        try:
            # winreg is only available on Windows.
            import winreg
        except ImportError:
            return False
        else:
            try:
                reg_key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Console")
                reg_key_value, _ = winreg.QueryValueEx(reg_key, "VirtualTerminalLevel")
            except FileNotFoundError:
                return False
            else:
                return reg_key_value == 1

    # isatty is not always implemented, #6223.
    is_a_tty = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    return is_a_tty and (
        sys.platform != "win32"
        or (HAS_COLORAMA and getattr(colorama, "fixed_windows_console", False))
        or "ANSICON" in os.environ
        or
        # Windows Terminal supports VT codes.
        "WT_SESSION" in os.environ
        or
        # Microsoft Visual Studio Code's built-in terminal supports colors.
        os.environ.get("TERM_PROGRAM") == "vscode"
        or vt_codes_enabled_in_windows_registry()
    )


class Style:
    pass


def make_style(config_string=""):
    """
    Create a Style object from the given config_string.

    If config_string is empty django.utils.termcolors.DEFAULT_PALETTE is used.
    """

    style = Style()
    color_settings = termcolors.parse_color_setting(config_string)

    # Avoid repeated attribute lookup and function creation in loop
    nocolor_roles = termcolors.PALETTES[termcolors.NOCOLOR_PALETTE].keys()
    nocolor = color_settings is None

    if not nocolor:
        # Pre-create style functions per unique format to minimize calls to make_style
        format_to_func = {}
        for role in nocolor_roles:
            fmt = color_settings.get(role, {})
            fmt_key = id(fmt)
            if fmt_key not in format_to_func:
                format_to_func[fmt_key] = termcolors.make_style(**fmt)
            setattr(style, role, format_to_func[fmt_key])
    else:
        # Use a shared identity function for all roles
        def identity(x):
            return x

        for role in nocolor_roles:
            setattr(style, role, identity)

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
    return make_style(os.environ.get("DJANGO_COLORS", ""))
