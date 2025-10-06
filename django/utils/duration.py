import datetime


def _get_duration_components(duration):
    days = duration.days
    seconds = duration.seconds
    microseconds = duration.microseconds

    minutes = seconds // 60
    seconds %= 60

    hours = minutes // 60
    minutes %= 60

    return days, hours, minutes, seconds, microseconds


def duration_string(duration):
    """Version of str(timedelta) which is not English specific."""
    days, hours, minutes, seconds, microseconds = _get_duration_components(duration)
    # Build the string efficiently with minimal allocations
    if days:
        # Most expensive formatting, but rare
        # Use f-strings for better performance
        if microseconds:
            # Days & microseconds (very rare)
            return f"{days} {hours:02d}:{minutes:02d}:{seconds:02d}.{microseconds:06d}"
        else:
            return f"{days} {hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        if microseconds:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{microseconds:06d}"
        else:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def duration_iso_string(duration):
    if duration < datetime.timedelta(0):
        sign = "-"
        duration *= -1
    else:
        sign = ""

    days, hours, minutes, seconds, microseconds = _get_duration_components(duration)
    ms = ".{:06d}".format(microseconds) if microseconds else ""
    return "{}P{}DT{:02d}H{:02d}M{:02d}{}S".format(
        sign, days, hours, minutes, seconds, ms
    )


def duration_microseconds(delta):
    return (24 * 60 * 60 * delta.days + delta.seconds) * 1000000 + delta.microseconds
