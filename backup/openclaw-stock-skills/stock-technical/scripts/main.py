#!/usr/bin/env python3
from __future__ import annotations

# Legacy skill entrypoint retained for compatibility; delegates to openclaw_skills.

import runpy
from pathlib import Path

TARGET = Path(__file__).resolve().parents[3] / 'openclaw_skills' / 'stock-technical' / 'scripts' / 'main.py'


if __name__ == '__main__':
    runpy.run_path(str(TARGET), run_name='__main__')
