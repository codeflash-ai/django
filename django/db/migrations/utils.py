import datetime
import re
from collections import namedtuple

from django.db.models.fields.related import RECURSIVE_RELATIONSHIP_CONSTANT

FieldReference = namedtuple("FieldReference", "to through")

COMPILED_REGEX_TYPE = type(re.compile(""))


class RegexObject:
    def __init__(self, obj):
        self.pattern = obj.pattern
        self.flags = obj.flags

    def __eq__(self, other):
        if not isinstance(other, RegexObject):
            return NotImplemented
        return self.pattern == other.pattern and self.flags == other.flags


def get_migration_name_timestamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M")


def resolve_relation(model, app_label=None, model_name=None):
    """
    Turn a model class or model reference string and return a model tuple.

    app_label and model_name are used to resolve the scope of recursive and
    unscoped model relationship.
    """
    if isinstance(model, str):
        if model == RECURSIVE_RELATIONSHIP_CONSTANT:
            if app_label is None or model_name is None:
                raise TypeError(
                    "app_label and model_name must be provided to resolve "
                    "recursive relationships."
                )
            return app_label, model_name
        dot_index = model.find(".")
        if dot_index != -1:
            app_label_, model_name_ = model[:dot_index], model[dot_index + 1 :]
            return app_label_, model_name_.lower()
        if app_label is None:
            raise TypeError(
                "app_label must be provided to resolve unscoped model relationships."
            )
        return app_label, model.lower()
    meta = model._meta
    return meta.app_label, meta.model_name


def field_references(
    model_tuple,
    field,
    reference_model_tuple,
    reference_field_name=None,
    reference_field=None,
):
    """
    Return either False or a FieldReference if `field` references provided
    context.

    False positives can be returned if `reference_field_name` is provided
    without `reference_field` because of the introspection limitation it
    incurs. This should not be an issue when this function is used to determine
    whether or not an optimization can take place.
    """
    # Localize for performance in tight loops
    remote_field = field.remote_field
    if not remote_field:
        return False
    references_to = None
    references_through = None

    # Only call resolve_relation once per relevant check
    remote_field_model = remote_field.model
    if resolve_relation(remote_field_model, *model_tuple) == reference_model_tuple:
        # Use direct attribute access where possible
        to_fields = getattr(field, "to_fields", None)
        if (
            reference_field_name is None
            or to_fields is None
            or (
                None in to_fields
                and (reference_field is None or reference_field.primary_key)
            )
            or (reference_field_name in to_fields)
        ):
            references_to = (remote_field, to_fields)

    # Avoid repeated getattr with a single lookup
    through = getattr(remote_field, "through", None)
    if through:
        if resolve_relation(through, *model_tuple) == reference_model_tuple:
            through_fields = remote_field.through_fields
            if (
                reference_field_name is None
                or through_fields is None
                or reference_field_name in through_fields
            ):
                references_through = (remote_field, through_fields)
    if not (references_to or references_through):
        return False
    return FieldReference(references_to, references_through)


def get_references(state, model_tuple, field_tuple=()):
    """
    Generator of (model_state, name, field, reference) referencing
    provided context.

    If field_tuple is provided only references to this particular field of
    model_tuple will be generated.
    """
    for state_model_tuple, model_state in state.models.items():
        for name, field in model_state.fields.items():
            reference = field_references(
                state_model_tuple, field, model_tuple, *field_tuple
            )
            if reference:
                yield model_state, name, field, reference


def field_is_referenced(state, model_tuple, field_tuple):
    """Return whether `field_tuple` is referenced by any state models."""
    return next(get_references(state, model_tuple, field_tuple), None) is not None
