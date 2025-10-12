from hashlib import md5

TEMPLATE_FRAGMENT_KEY_TEMPLATE = "template.cache.%s.%s"


def make_template_fragment_key(fragment_name, vary_on=None):
    hasher = md5(usedforsecurity=False)
    if vary_on is not None:
        # Pre-join for fewer .update() calls and less encode overhead
        joined = ":".join(str(arg) for arg in vary_on)
        # Ensure trailing ':' to match previous logic (extra ':' after last item)
        if vary_on:
            joined += ":"
        hasher.update(joined.encode())
    return TEMPLATE_FRAGMENT_KEY_TEMPLATE % (fragment_name, hasher.hexdigest())
