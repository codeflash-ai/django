from hashlib import md5

TEMPLATE_FRAGMENT_KEY_TEMPLATE = "template.cache.%s.%s"


def make_template_fragment_key(fragment_name, vary_on=None):
    hasher = md5(usedforsecurity=False)
    if vary_on is not None:
        # Combine all vary_on items as strings, separated by colons, then encode and update once
        combined = ":".join(str(arg) for arg in vary_on)
        hasher.update(combined.encode())
        if vary_on:  # Only add trailing ":" if vary_on is not empty
            hasher.update(b":")
    return TEMPLATE_FRAGMENT_KEY_TEMPLATE % (fragment_name, hasher.hexdigest())
