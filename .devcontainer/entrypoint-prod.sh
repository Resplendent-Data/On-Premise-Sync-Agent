#!/bin/bash
# entrypoint.sh

# Check and copy the prod.env.json file to the /app directory as env.json, if it doesn't exist
if [ ! -f /app/sync_agent_configs/env.json ]; then
    cp envs/prod.env.json /app/sync_agent_configs/env.json
fi

# Check and copy the default.sync_agent.json file to the /app directory as sync_agent.json, if it doesn't exist
if [ ! -f /app/sync_agent_configs/sync_agent.json ]; then
    cp default_configs/default.sync_agent.json /app/sync_agent_configs/sync_agent.json
fi

# Execute the command provided as arguments to this script
exec "$@"
