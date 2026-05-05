"""Declarative recipes -- discovered at runtime by ``operator recipe ...``.

Each ``*.py`` here defines one or more ``Recipe`` subclasses and either:
- decorates the class with ``@register_recipe``, or
- relies on ``discover_recipes()`` to pick it up.

To add a new recipe: drop a file here, set ``name``/``version``, implement
the lifecycle hooks, and ``operator recipe list`` will pick it up.
"""
