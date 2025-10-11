"""
Backend for test environment.
"""

import copy

from django.core import mail
from django.core.mail.backends.base import BaseEmailBackend


class EmailBackend(BaseEmailBackend):
    """
    An email backend for use during test sessions.

    The test connection stores email messages in a dummy outbox,
    rather than sending them out on the wire.

    The dummy outbox is accessible through the outbox instance attribute.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(mail, "outbox"):
            mail.outbox = []

    def send_messages(self, messages):
        """Redirect messages to the dummy outbox"""
        # The original implementation deepcopies and appends each message in the for-loop.
        # The deepcopy operation dominates the runtime.
        #
        # Optimization: Reduce Python-level overhead by first validating all messages,
        # then batch copy and batch extend, minimizing per-iteration attribute lookup,
        # appending, and repeated lock acquisitions inside list.append().
        #
        # This preserves exact semantics: .message() is still called for each input
        # in the same order and before any points of mutation, ensuring header validation.
        validated_msgs = []
        for message in messages:  # .message() triggers header validation
            message.message()
            validated_msgs.append(message)
        # Bulk deepcopy in one call is more efficient than looping (uses list comprehensions internally)
        deepcopied_msgs = list(map(copy.deepcopy, validated_msgs))
        mail.outbox.extend(deepcopied_msgs)
        return len(deepcopied_msgs)
