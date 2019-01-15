#!/usr/bin/env sh
cd /github/workspace
pip install pytest

if [ -f requirements.txt ]; then
    pip install -r requirements.txt
fi

py.test -v