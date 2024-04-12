import json
import os
import uuid
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import sqliteDB_setup

creds = {}

# Has environment information like whether the program is in debug or not
env_items = json.load(open("sync_agent_configs/env.json"))
if env_items["debug"]:
    config_path = "./sync_agent_configs"
else:
    config_path = "./sync_agent_configs"

# Used to keep track of when the next refresh of data will be.
timer = 0


# version of the sync agent
def get_version():
    f = open("versioning/version.txt", "r")
    version = f.read()
    f.close()
    return version


VERSION = get_version()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

server = app.server

app.config.suppress_callback_exceptions = True
app.title = "Sync Agent"
app.layout = html.Div(
    [
        dbc.Button(
            id="login_button",
            children="Log In",
            className="mt-2",
        ),
    ],
    id="main",
)


def update_source_statuses(connection_df, error):
    con_rows = []

    for _, value in connection_df.iterrows():
        if value["connection_status"] == "True" and error["status"]:
            connection_status = html.Td(
                "Connected", style={"backgroundColor": "#45ab5d"}
            )
        else:
            connection_status = html.Td(
                "Disconnected", style={"backgroundColor": "#d9423c"}
            )
        if error["status"] == False:
            if error["type"] == "authentication":
                con_rows.append(
                    html.Tr(
                        [
                            html.Td(value["connection_name"]),
                            html.Td("Agent not authenticated"),
                            connection_status,
                        ]
                    )
                )
            elif error["type"] == "connection":
                con_rows.append(
                    html.Tr(
                        [
                            html.Td(value["connection_name"]),
                            html.Td("Agent not connected"),
                            connection_status,
                        ]
                    )
                )
            elif error["type"] == "general":
                con_rows.append(
                    html.Tr(
                        [
                            html.Td(value["connection_name"]),
                            html.Td("Agent failing"),
                            connection_status,
                        ]
                    )
                )
        else:
            con_rows.append(
                html.Tr(
                    [
                        html.Td(value["connection_name"]),
                        html.Td(value["connection_error"]),
                        connection_status,
                    ]
                )
            )

    table_body = html.Tbody(con_rows)
    table_header = html.Thead(
        html.Tr([html.Th("Connection"), html.Th("Error Message"), html.Th("Status")])
    )

    table = dbc.Table(
        # using the same table as in the above example
        children=[table_header, table_body],
        bordered=True,
        dark=True,
        hover=True,
        responsive=True,
        striped=True,
    )

    return table


def generate_sync_time():
    sync_info = sqliteDB_setup.create_connection(
        "sync_info.db", sqliteDB_setup.generate_select("sync_info"), return_values=True
    )
    # Get latest time from the database
    sync_time = sync_info.iloc[[-1]]["sync_time"].values[0]
    last_update = datetime.strptime(
        sync_info.iloc[[-1]]["last_update"].values[0], "%Y-%m-%d %H:%M:%S"
    )
    if sync_time == -1:
        sync_time = html.H4("No historical syncs")

    else:
        time_ago = datetime.utcnow() - last_update
        sync_time = dcc.Markdown(
            f"""
            #### Sync Time: {round(sync_time, 5)} {'Second' if round(sync_time, 5) == 1 else 'Seconds'}
            _Last updated **{int(time_ago.total_seconds())}** seconds ago (should be less than 120 seconds)_
            """
        )
    sync_info.rename(
        columns={"sync_time": "Sync Time", "last_update": "Sync Date"}, inplace=True
    )
    return sync_time, sync_info


def agent_info(agent_uuid, agent_key, connection_df, sync_time, sync_time_df, error):
    """Initiates the agent info and source statuses"""
    agent_info = html.Div(
        dbc.Card(
            [
                dbc.CardHeader(
                    [
                        dbc.Row(
                            [
                                dbc.Col(["Agent Creds"]),
                                dbc.Col(
                                    [f"V. {VERSION}"], style={"textAlign": "right"}
                                ),
                            ]
                        )
                    ]
                ),
                dbc.CardBody(
                    [
                        html.Div(
                            [
                                dcc.Markdown(
                                    f"""
                            ##### Current Agent Creds

                            Agent UUID 
                            
                            `{agent_uuid}`

                            Agent Key 
                            
                            `{agent_key}`
                            """,
                                    id="agent-info",
                                ),
                            ],
                            style={"textAlign": "left", "width": "100%"},
                        ),
                        dbc.InputGroup(
                            [
                                dbc.Input(
                                    id="agent_uuid",
                                    placeholder="Agent UUID",
                                ),
                                dbc.Input(
                                    id="agent_key",
                                    placeholder="Agent Key",
                                ),
                            ]
                        ),
                        dbc.Button(
                            id="set_creds_button",
                            children="Set Creds",
                            className="mt-2",
                        ),
                    ],  # style={'display': 'flex', 'flexDirection': 'column', 'alignItems': 'center', 'justifyContent': 'center'},
                    id="card_body",
                ),
            ]
        )
    )

    table = update_source_statuses(connection_df, error)

    fig = px.line(
        sync_time_df,
        x="Sync Date",
        y="Sync Time",
        # title='Sync time over past 30 days',
    )
    fig.update_layout(
        {
            "plot_bgcolor": "rgba(0, 0, 0, 0)",
            "paper_bgcolor": "rgba(0, 0, 0, 0)",
            "margin": dict(l=0, r=0, t=30, b=0),
            "font_color": "white",
            "xaxis": dict(showgrid=False),
            "yaxis": dict(showgrid=False),
        }
    )

    last_sync_time = html.Div(
        [
            dbc.Card(
                [
                    dbc.CardHeader(["Manage"]),
                    dbc.CardBody(
                        [
                            dbc.Button("Reboot", id="reboot_btn", className="mt-2"),
                            html.Div("", id="rebooting"),
                        ]
                    ),
                ]
            ),
            html.Br(),
            dbc.Card(
                [
                    dbc.CardHeader(["Overall Sync Time"]),
                    dbc.CardBody(
                        [
                            html.Div(sync_time, id="sync-time"),
                            dcc.Graph(id="sync-time-graph", figure=fig),
                        ],
                    ),
                ]
            ),
        ]
    )

    sources_connected = html.Div(
        [
            dbc.Card(
                [
                    dbc.CardHeader("Connections"),
                    dbc.CardBody(
                        [
                            html.H4(
                                id="table-timer",
                            ),
                            html.Br(),
                            html.H5(
                                "Agent Errors",
                            ),
                            html.Div(
                                html.P(
                                    error["message"],
                                    style={
                                        "border": f'{"0px" if error["status"] else "5px"} #d9423c solid'
                                    },
                                ),
                                id="general-errors",
                            ),
                            html.Hr(),
                            html.H5("Data Sources"),
                            html.Div([table], id="source-table"),
                        ],
                    ),
                ]
            ),
            dcc.Interval(id="table-refresh", interval=1000, n_intervals=0),
        ]
    )

    # All of the agent info placed into one layout
    info_collection = [
        dbc.Container(
            [
                dcc.Loading(
                    id="rebooting-loading",
                    type="cube",
                    fullscreen=True,
                    style={"backgroundColor": "#303030"},
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                agent_info,
                                html.Br(),
                                sources_connected,
                            ]
                        ),
                        dbc.Col(
                            [last_sync_time],
                        ),
                    ]
                ),
            ],
            fluid=True,
            style={"marginTop": "30px"},
        )
    ]

    return info_collection


@app.callback(
    Output("rebooting-loading", "children"),
    Input("reboot_btn", "n_clicks"),
)
def reboot(n_clicks):
    if n_clicks != None:
        try:
            os.system("echo b > /sysrq")
            return "Rebooting..."
        except Exception as e:
            return str(e)


@app.callback(
    Output("agent-info", "children"),
    Input("set_creds_button", "n_clicks"),
    [State("agent_uuid", "value"), State("agent_key", "value")],
)
def set_agent_creds(n_clicks, agent_uuid, agent_key):
    if n_clicks != None:
        try:
            if os.path.isfile(f"{config_path}/sync_agent.json"):
                with open(f"{config_path}/sync_agent.json") as f:
                    config = json.load(f)

                if len(config) == 0:
                    config = {"dbkey": uuid.uuid4().hex + uuid.uuid4().hex}
                    config["uuid"] = agent_uuid
                    config["key"] = agent_key
                    with open(f"{config_path}/sync_agent.json", "w") as f:
                        f.write(json.dumps(config))
                        sqliteDB_setup.create_connection(
                            "sync_info.db", sqliteDB_setup.update_command("restart")
                        )

                else:
                    config["uuid"] = agent_uuid
                    config["key"] = agent_key
                    with open(f"{config_path}/sync_agent.json", "w") as f:
                        f.write(json.dumps(config))
                        sqliteDB_setup.create_connection(
                            "sync_info.db", sqliteDB_setup.update_command("restart")
                        )
            else:
                config = {"dbkey": uuid.uuid4().hex + uuid.uuid4().hex}
                config["uuid"] = agent_uuid
                config["key"] = agent_key
                with open(f"{config_path}/sync_agent.json", "w") as f:
                    f.write(json.dumps(config))
                    sqliteDB_setup.create_connection(
                        "sync_info.db", sqliteDB_setup.update_command("restart")
                    )

            # os.system('systemctl restart sync_agent')
            return f"""
                    ##### Current Agent Creds

                    Agent UUID 
                    
                    `{agent_uuid}`

                    Agent Key 
                    
                    `{agent_key}`
                    """
        except:
            return html.P("Failed to save :(")
    else:
        raise PreventUpdate


@app.callback(
    Output("main", "children"),
    Input("login_button", "n_clicks"),
)
def login(n_clicks):
    # Dataframe from the sqlite database
    connection_statuses = sqliteDB_setup.create_connection(
        "sync_info.db",
        sqliteDB_setup.generate_select("connection_info"),
        return_values=True,
    )
    sync_time, sync_time_df = generate_sync_time()
    auth_status = sqliteDB_setup.create_connection(
        "sync_info.db", sqliteDB_setup.generate_select("agent_errors"), True
    ).values[0][1]
    connection_status = sqliteDB_setup.create_connection(
        "sync_info.db", sqliteDB_setup.generate_select("agent_errors"), True
    ).values[1][1]
    agent_status = sqliteDB_setup.create_connection(
        "sync_info.db", sqliteDB_setup.generate_select("agent_errors"), True
    ).values[2][1]

    error = {"type": None, "status": True, "message": "No errors"}
    if connection_status == "Not connected":
        error["type"] = "connection"
        error["status"] = False
        error[
            "message"
        ] = "Agent cannot reach the Resplendent servers. Please validate that the agent can resolve https://api.resplendentdata.com"
    elif auth_status == "Not Authenticated":
        error["type"] = "authentication"
        error["status"] = False
        error[
            "message"
        ] = "Agent not authenticated. The entered Agent creds are invalid"
    elif agent_status != "Ready":
        error["type"] = "general"
        error["status"] = False
        error["message"] = agent_status

    if os.path.isfile(f"{config_path}/sync_agent.json"):
        with open(f"{config_path}/sync_agent.json", "r") as f:
            config = json.load(f)
        if len(config) == 0:
            return agent_info(
                None, None, connection_statuses, sync_time, sync_time_df, error
            )
        return agent_info(
            config["uuid"],
            config["key"],
            connection_statuses,
            sync_time,
            sync_time_df,
            error,
        )
    else:
        return agent_info(
            None, None, connection_statuses, sync_time, sync_time_df, error
        )


@app.callback(
    Output("source-table", "children"),
    Output("sync-time", "children"),
    Output("sync-time-graph", "figure"),
    Output("general-errors", "children"),
    [Input("table-refresh", "n_intervals")],
)
def table_updater(n):
    global timer
    if timer == 0:
        connection_statuses = sqliteDB_setup.create_connection(
            "sync_info.db",
            sqliteDB_setup.generate_select("connection_info"),
            return_values=True,
        )
        sync_time, sync_time_df = generate_sync_time()
        auth_status = sqliteDB_setup.create_connection(
            "sync_info.db", sqliteDB_setup.generate_select("agent_errors"), True
        ).values[0][1]
        connection_status = sqliteDB_setup.create_connection(
            "sync_info.db", sqliteDB_setup.generate_select("agent_errors"), True
        ).values[1][1]
        agent_status = sqliteDB_setup.create_connection(
            "sync_info.db", sqliteDB_setup.generate_select("agent_errors"), True
        ).values[2][1]

        error = {"type": None, "status": True, "message": "No errors"}
        if connection_status == "Not connected":
            error["type"] = "connection"
            error["status"] = False
            error[
                "message"
            ] = "Agent cannot reach the Resplendent servers. Please validate that the agent can resolve https://api.resplendentdata.com"
        elif auth_status == "Not Authenticated":
            error["type"] = "authentication"
            error["status"] = False
            error[
                "message"
            ] = "Agent not authenticated. The entered Agent creds are invalid"
        elif agent_status != "Ready":
            error["type"] = "general"
            error["status"] = False
            error["message"] = agent_status

        fig = px.line(
            sync_time_df,
            x="Sync Date",
            y="Sync Time",
            # title='Sync time over past 30 days',
        )
        fig.update_layout(
            {
                "plot_bgcolor": "rgba(0, 0, 0, 0)",
                "paper_bgcolor": "rgba(0, 0, 0, 0)",
                "margin": dict(l=0, r=0, t=30, b=0),
                "font_color": "white",
                "xaxis": dict(showgrid=False),
                "yaxis": dict(showgrid=False),
            }
        )

        error_message = html.P(
            error["message"],
            style={"border": f'{"0px" if error["status"] else "5px"} #d9423c solid'},
        )

        return [
            update_source_statuses(connection_statuses, error),
            sync_time,
            fig,
            error_message,
        ]
    else:
        raise PreventUpdate


@app.callback(
    Output("table-timer", "children"), [Input("table-refresh", "n_intervals")]
)
def display_timer(n):
    global timer
    if timer == 0:
        timer = 5
        return html.Div(
            dbc.Spinner(
                color="light", spinner_style={"width": "25px", "height": "25px"}
            ),
        )

    else:
        timer -= 1

    return f"Refreshing in: {timer}"


if __name__ == "__main__":
    app.run_server("0.0.0.0", "8050", debug=True, dev_tools_silence_routes_logging=True)
