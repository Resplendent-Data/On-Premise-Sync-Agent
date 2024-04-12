#!/bin/bash
exec poetry run python sync_agent.py &
exec poetry run python -m gunicorn -b 0.0.0.0:8050 config_server:server