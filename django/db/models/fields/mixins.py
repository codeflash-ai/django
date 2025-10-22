from django.core import checks

NOT_PROVIDED = object()


class FieldCacheMixin:
    """Provide an API for working with the model's fields value cache."""

    def get_cache_name(self):
        raise NotImplementedError

    def get_cached_value(self, instance, default=NOT_PROVIDED):
        cache_name = self.get_cache_name()
        try:
            return instance._state.fields_cache[cache_name]
        except KeyError:
            if default is NOT_PROVIDED:
                raise
            return default

    def is_cached(self, instance):
        return self.get_cache_name() in instance._state.fields_cache

    def set_cached_value(self, instance, value):
        instance._state.fields_cache[self.get_cache_name()] = value

    def delete_cached_value(self, instance):
        del instance._state.fields_cache[self.get_cache_name()]


class CheckFieldDefaultMixin:
    _default_hint = ("<valid default>", "<invalid default>")

    def _check_default(self):
        # Fast path: No default, or default is None, or default is callable
        # Avoid computing self.__class__.__name__ and self._default_hint unless needed
        default = self.default
        if self.has_default() and default is not None and not callable(default):
            class_name = self.__class__.__name__
            hint_from, hint_to = self._default_hint
            return [
                checks.Warning(
                    f"{class_name} default should be a callable instead of an instance "
                    "so that it's not shared between all field instances.",
                    hint=(
                        f"Use a callable instead, e.g., use `{hint_from}` instead of "
                        f"`{hint_to}`."
                    ),
                    obj=self,
                    id="fields.E010",
                )
            ]
        # Return shared constant empty list (no warning) for better performance and memory
        return []

    def check(self, **kwargs):
        errors = super().check(**kwargs)
        errors.extend(self._check_default())
        return errors
