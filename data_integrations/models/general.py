"""General Models for Data Integrations"""

from __future__ import annotations

from typing import Dict, List, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


def to_camel(string: str) -> str:
    """Convert a string to camel case."""
    camel_cased = "".join(word.capitalize() for word in string.split("_"))
    # Lowercase the first letter
    return camel_cased[0].lower() + camel_cased[1:]


class ColumnLinkSection(BaseModel):
    """
    Column links will be sent in the format of:
    {
        "column_name": [
            {
                "type": "column" | "string",
                "value": "" | "string_value"
            }
        ]
    }
    The value is blank if the type is "column" so it
    can be filled in later.
    """

    type: str = Field(..., alias="type")
    value: str = Field(..., alias="value")

    @field_validator("type")
    @classmethod
    def type_validator(cls, v):
        """Check that the type is either column or string."""
        if v not in ["column", "string"]:
            raise ValueError("type must be either 'column' or 'string'")
        return v

    @model_validator(mode="before")
    @classmethod
    def value_validator(cls, values):
        """Check if the value is not blank if the type is 'string'"""
        if values["type"] == "string":
            if not values["value"]:
                raise ValueError("value must be filled in if type is 'column'")
        return values


class TableRequirements(BaseModel):
    """
    The generic requirements for returned tables when calling for
    a table preview
    """

    primary_key: str | None = Field(
        default=None, description="The primary key of the table"
    )
    last_update: str | None = Field(
        default=None, description="The last update date of the table"
    )
    deleted_rows_sort_column: Literal["primary_key", "last_update"] = Field(
        default="last_update",
        description="The column to sort deleted rows by can be either the last update or primary key column",
    )
    required_relevant_columns: List[str] = Field(
        [], description="The columns that are required to be returned"
    )
    prebuilt_column_links: Dict[str, List[ColumnLinkSection]] = Field(
        default=[], description="The columns that are prebuilt and should be linked"
    )
    column_aliases: Dict[str, str] = Field(
        default={}, description="The aliases for the columns"
    )
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


def text_field_limit(value: str):
    """Limit the length of a text field to 256 characters"""
    max_length = 128
    if len(value) > max_length:
        return value[: max_length - 3] + "..."
    return value


if __name__ == "__main__":
    a = TableRequirements(
        primary_key="id",
        last_update="lastUpdate",
        required_relevant_columns=["id", "name"],
    )
    print(a.json(by_alias=True))
