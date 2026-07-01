"""Rend le package ``ead_editor`` importable sans installation préalable."""

import sys
from pathlib import Path

# Le dossier parent de tests/ contient le package ead_editor/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
