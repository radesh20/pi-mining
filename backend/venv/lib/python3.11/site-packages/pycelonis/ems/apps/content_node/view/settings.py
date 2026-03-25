from typing import Optional, List, Union

from pycelonis_core.base.base_model import PyCelonisBaseModel
from saolapy.pql.base import PQLFilter

try:
    from pydantic.v1 import Extra, Field  # type: ignore
except ImportError:
    from pydantic import Extra, Field  # type: ignore


class ViewComponentDataSourceAttribute(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view component data source attribute."""

    id: str
    """Id of attribute."""
    pql: str
    """PQL of attribute."""
    column_type: Optional[str] = None
    """Type of column (e.g. dimension, measure)."""
    description: Optional[str] = None
    """Description of attribute."""
    display_name: Optional[str] = None
    """Display name of attribute."""

    def name(self, i: int = 0) -> str:
        """Returns name of data source attribute."""
        return self.display_name or f"New Expression {i}"


class ViewComponentDataSourceFilter(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view component data source filter."""

    pql: str
    """PQL of filter."""
    is_referenced: bool = Field(default=False)
    """Whether filter is reference from knowledge model."""

    def to_pql(self) -> PQLFilter:
        """Returns PQL filter. For filters referenced from knowledge model, syntax is adjusted."""
        return PQLFilter(query=f"FILTER @{self.pql};") if self.is_referenced else PQLFilter(query=self.pql)


class ViewComponentDataSource(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view component data source."""

    id: str
    """Id of data source."""
    description: Optional[str] = None
    """Description of data source."""
    display_name: Optional[str] = None
    """Display name of data source."""
    attributes: List[ViewComponentDataSourceAttribute] = Field(default_factory=list)
    """Attributes of data source, e.g. dimensions and kpis."""
    filters: List[ViewComponentDataSourceFilter] = Field(default_factory=list)
    """Filters of data source, e.g. filter attributes."""


class ViewComponentColumn(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view component data column."""

    id: str
    order: Optional[int]


class ViewComponentDataColumn(ViewComponentColumn, extra=Extra.allow):  # type: ignore
    """Class for view component data column."""

    field: str


class ViewComponentSortByDataColumn(ViewComponentDataColumn, extra=Extra.allow):  # type: ignore
    """Class for view component data column."""

    direction: Optional[str] = Field(default="ASC")


class ViewComponentKpiDataColumn(ViewComponentColumn, extra=Extra.allow):  # type: ignore
    """Class for view component data column."""

    kpi: str


class ViewComponentData(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view component data."""

    sort_by: List[ViewComponentSortByDataColumn] = Field(default_factory=list)


class ComponentSettings(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view component settings."""

    data: Union[ViewComponentData, List] = Field(default_factory=ViewComponentData)
    data_sources: List[ViewComponentDataSource] = Field(default_factory=list)
