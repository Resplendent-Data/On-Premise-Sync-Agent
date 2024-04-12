from types import ModuleType
from typing import Dict, Union
import data_integrations.mssql as mssql
import data_integrations.mysql as mysql
import data_integrations.postgresql as postgresql
from data_integrations.template import IntegrationSyncFunctions

integration_map: Dict[str, Union[IntegrationSyncFunctions, ModuleType]] = {
    "mysql": mysql,
    "postgresql": postgresql,
    "mssql": mssql,
}
