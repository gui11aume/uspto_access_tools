#!/usr/bin/env python3
"""Thin wrapper for downloading USPTO APPXML bulk files."""

from __future__ import annotations

import sys

from bulk_download import main_for_product


if __name__ == "__main__":
    sys.exit(main_for_product("APPXML", "appxml"))
