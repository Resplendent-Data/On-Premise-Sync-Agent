# sync-agent

### env.json for local dev

```json
{
  "debug": true,
  "url": "ws://slave-driver:8001/slave-driver/websocket/"
}
```

### sync_agent.json for local dev

```json
{
  "dbkey": "42069lolz",
  "uuid": "d872b2ab-4f28-4643-8da9-f55cf2b6010e",
  "key": "880cfb2b67854b7890d908c562db990b8428be6182a041b5948ed580f48113f8"
}
```

## Running the sync agent

If you're using VS code for development, you can open any python file and press F5 or Ctrl+F5 to run the file. If you're not using VS code, you can run the sync agent by running the following command in the root directory of the project:

```bash
python sync_agent.py
```

