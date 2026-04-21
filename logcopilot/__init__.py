from __future__ import annotations

"""LogCopilot public package API."""

import sys
from importlib import import_module

from .runtime import run_profile

__all__ = ["__version__", "run_profile"]

__version__ = "0.1.0"

_COMPAT_MODULES = {
    "cli": ".pipeline",
    "clustering": ".analysis.clustering",
    "models": ".domain.models",
    "normalization": ".text.normalization",
    "quality": ".analysis.quality",
    "reporting": ".output.reporting",
    "service": ".runtime",
    "semantic": ".analysis.semantic",
    "signatures": ".text.signatures",
}

for _alias, _target in _COMPAT_MODULES.items():
    sys.modules.setdefault(f"{__name__}.{_alias}", import_module(_target, __name__))
