#!/usr/bin/env bash
cd /github/workspace
pip install pytest

if [ -f requirements.txt ]; then
    pip install -r requirements.txt
fi

py.test -v