#!/bin/bash
# entrypoint.sh

# Copy the dev.env.json file to the /workspace directory as env.json
cp envs/dev.env.json /workspace/env.json

# Copy the dev.sync_agent.json file to the /workspace directory as sync_agent.json
cp default_configs/dev.sync_agent.json /workspace/sync_agent.json

# Execute the command provided as arguments to this script
exec "$@"
