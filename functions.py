"""
General helper functions for the sync agent and
and its components.
"""

import inspect
import sys
import traceback
from datetime import datetime
from inspect import getframeinfo, stack
from typing import Type, get_args, get_origin

import pandas as pd
from pydantic import BaseModel


def log(*args):
    """
    Logs the message to the console,
    showing the current time and the
    message. It also logs which file
    the message is from.
    """
    caller = getframeinfo(stack()[1][0])
    print(f"{datetime.now()}: {caller.filename}:{caller.lineno} -", *args, flush=True)
    return None


def trim_string(string: str, max_length: int) -> str:
    """
    Trims a string to a maximum length. Used in Pydantic models.
    Add an ellipsis if the string is too long.
    """
    if len(string) > max_length:
        return string[: max_length - 3] + "..."
    return string


def log_error(e: Exception, reraise=False):
    """Log the error and optionally reraise it"""
    _, _, exc_tb = sys.exc_info()
    if exc_tb is not None:
        traceback.print_exception(e)
    else:
        print(f"Error with no trace: {e}", flush=True)
    if reraise:
        raise Exception(e)


def df_to_dict(df: pd.DataFrame, table_object: dict | None = None) -> dict:
    """convert df to dict for json serialization"""
    # Covert all object columns to string
    for col in df.select_dtypes(include=["object"]).columns:
        col = str(col)
        # Convert where value is not null
        df.loc[df[col].notnull(), col] = df.loc[df[col].notnull(), col].astype(str)

    # Convert tz-naive datetime columns to UTC or user specified timezone
    if table_object is not None:
        for col, dtype in zip(df.columns, df.dtypes):
            if str(dtype) == "datetime64[ns]":
                if (
                    table_object["column_timezones"] is not None
                    and col in table_object["column_timezones"]
                ):
                    df[col] = df[col].dt.tz_localize(
                        table_object["column_timezones"][col],
                        ambiguous="infer",
                        nonexistent="shift_backward",
                    )
                else:
                    df[col] = df[col].dt.tz_localize(
                        "UTC", ambiguous="infer", nonexistent="shift_backward"
                    )

    df_dict = {
        "values": df.to_json(
            orient="values",
            date_format="iso",
            default_handler=str,
            date_unit="s",
        ),
        "columns": df.columns.tolist(),
        "dtypes": list(df.dtypes.astype(str)),
    }

    return df_dict


def check_if_contains_pydantic(field_annotation: Type):
    """Recursively checks if a given type contains a pydantic model"""

    if inspect.isclass(field_annotation) and issubclass(field_annotation, BaseModel):
        return field_annotation
    elif len(get_args(field_annotation)) > 0:
        for arg in get_args(field_annotation):
            if check_if_contains_pydantic(arg):
                # Return the first pydantic model found
                return check_if_contains_pydantic(arg)
    return None


def recursively_check_if_contains_child_type(
    field_annotation: Type, origin: Type | None = None
):
    """
    Checks if a given type contains more than 0 args and
    if there are more than 0 args, recursively checks if
    those args contain more than 0 args
    """
    if len(get_args(field_annotation)) > 0:
        return recursively_check_if_contains_child_type(
            get_args(field_annotation)[0], get_origin(field_annotation)
        )
    else:
        return field_annotation, origin


def pydantic_to_dataframe(model: Type[BaseModel]):
    """Converts a pydantic model to a dictionary of column names and types"""
    data: dict[str, Type] = {}
    model_fields = model.model_fields
    for field_name, field in model_fields.items():
        if (pydantic_model := check_if_contains_pydantic(field.annotation)) is not None:
            sub_data = pydantic_to_dataframe(pydantic_model)
            for sub_field_name, sub_field in sub_data.items():
                data[f"{field_name}.{sub_field_name}"] = sub_field
            data[field_name] = str
        elif get_origin(field.annotation) is list:
            data[field_name] = str
        else:
            child_annotation, origin = recursively_check_if_contains_child_type(
                field.annotation
            )
            if origin is list:
                data[field_name] = str
            else:
                data[field_name] = child_annotation
    return data


def set_dataframe_types(df: pd.DataFrame, model):
    """Sets the types of the columns in the dataframe to match the types in the pydantic model"""
    if df.empty:
        return df
    data = pydantic_to_dataframe(model)
    for field_name, field_type in data.items():
        if field_name in df.columns:
            if issubclass(field_type, bool):
                df[field_name] = df[field_name].astype(field_type)
            elif issubclass(field_type, int):
                df[field_name] = df[field_name].astype("Int64")
            elif field_type == datetime:
                df[field_name] = pd.to_datetime(df[field_name], utc=True)
            elif field_type == str:
                df[field_name] = df[field_name].apply(
                    lambda x: str(x) if x is not None else None
                )
            else:
                df[field_name] = df[field_name].astype(field_type)
    return df


def build_column_aliases(model: type[BaseModel]) -> dict[str, str]:
    """Converts the Aliases of a Pydantic model to a dictionary of column names and aliases"""

    aliases = {}
    for field_name, field_info in model.model_fields.items():
        # If the field has an alias, add it to the aliases dictionary
        if field_info.alias is not None and field_info.alias != field_name:
            aliases[field_name] = field_info.alias
        # If the field has a sub-model, recursively call this function
        elif (
            field_info.annotation is not None
            and isinstance(field_info.annotation, type)
            and issubclass(field_info.annotation, BaseModel)
        ):
            sub_aliases = build_column_aliases(field_info.annotation)
            for sub_field_name in sub_aliases.keys():
                aliases[f"{field_name}.{sub_field_name}"] = sub_aliases[sub_field_name]
    return aliases


class TableAlreadyProcessingData(Exception):
    """An error class used to communicate that the table is already processing data"""


class UnboundSessionError(Exception):
    """Raised when a SQLAlchemy session has no associated engine (bind)."""


if __name__ == "__main__":
    log("Which one of you flatfoots stole my lollipop")
