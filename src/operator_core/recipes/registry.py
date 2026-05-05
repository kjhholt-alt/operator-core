"""In-process recipe registry + filesystem discovery.

Recipes register themselves either by:
1. Calling ``register_recipe(MyRecipe)`` at import time, or
2. Living in the top-level ``recipes/`` directory and being discovered.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import logging
from pathlib import Path
from typing import Iterable

from .base import Recipe

logger = logging.getLogger("operator.recipe.registry")

_REGISTRY: dict[str, type[Recipe]] = {}


def register_recipe(cls: type[Recipe]) -> type[Recipe]:
    """Decorator / function. Adds ``cls`` to the in-process registry by name."""
    if not getattr(cls, "name", ""):
        raise ValueError(f"recipe {cls.__name__} has no name")
    if cls.name in _REGISTRY and _REGISTRY[cls.name] is not cls:
        logger.warning("recipe.registry.overwrite", extra={"name": cls.name})
    _REGISTRY[cls.name] = cls
    return cls


def get_registered_recipe(name: str) -> type[Recipe] | None:
    return _REGISTRY.get(name)


def list_registered_recipes() -> list[type[Recipe]]:
    return sorted(_REGISTRY.values(), key=lambda c: c.name)


def clear_registry() -> None:
    """Test helper: drop all registered recipes."""
    _REGISTRY.clear()


def discover_recipes(directory: Path | str) -> list[type[Recipe]]:
    """Import every ``*.py`` under ``directory`` and collect ``Recipe`` subclasses.

    Side effects: each module is imported, which triggers any
    ``register_recipe`` calls inside.
    """
    base = Path(directory)
    if not base.exists():
        return []

    found: list[type[Recipe]] = []
    for path in sorted(base.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"_operator_recipe_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            continue
        try:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        except Exception as exc:  # noqa: BLE001
            logger.error("recipe.discover.import_failed", extra={"path": str(path), "error": str(exc)})
            continue
        for _, obj in inspect.getmembers(mod, inspect.isclass):
            if obj is Recipe:
                continue
            if issubclass(obj, Recipe) and getattr(obj, "name", ""):
                register_recipe(obj)
                found.append(obj)
    return found


def loaded_names() -> Iterable[str]:
    return list(_REGISTRY.keys())
