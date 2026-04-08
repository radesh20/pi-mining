from abc import ABC
from typing import List, ClassVar

from pycelonis.ems.apps.content_node.view.settings import ComponentSettings
from pycelonis_core.base.base_model import PyCelonisBaseModel
from saolapy.pql.base import PQLColumn, PQLFilter, OrderByColumn, PQL

try:
    from pydantic.v1 import Extra, Field  # type: ignore
except ImportError:
    from pydantic import Extra, Field  # type: ignore


class Component(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view components."""

    id: str
    """Id of component."""
    type_: str = Field(..., alias="type")
    """Type of component (e.g. table)."""
    settings: ComponentSettings = Field(default_factory=ComponentSettings)
    """Settings of component."""

    def get_query(self) -> PQL:
        """Returns full query with column, filter, and sorting for component.

        Returns:
            A PQL query.
        """
        return PQL() + self.get_columns() + self.get_filters() + self.get_order_by_columns()

    def get_columns(self) -> List[PQLColumn]:
        """Returns all columns of component including dimensions and kpis.

        Returns:
            List of PQLColumns.
        """
        raise NotImplementedError(f"`get_columns` is not available for component of type `{self.type_}`.")

    def get_filters(self) -> List[PQLFilter]:
        """Returns filters of component.

        Returns:
            Component filters.
        """
        raise NotImplementedError(f"`get_filters` is not available for component of type `{self.type_}`.")

    def get_order_by_columns(self) -> List[OrderByColumn]:
        """Returns order by columns of component.

        Returns:
            Order by columns of component.
        """
        raise NotImplementedError(f"`get_order_by_columns` is not available for component of type `{self.type_}`.")


class DataComponent(Component, ABC):
    """Component that displays data."""

    def get_columns(self) -> List[PQLColumn]:
        """Returns all columns of component including dimensions and kpis.

        Returns:
            List of PQLColumns.
        """
        data_source_attributes = [
            data_source_attribute
            for data_source in self.settings.data_sources
            if data_source is not None
            for data_source_attribute in data_source.attributes
            if data_source_attribute is not None
        ]
        return [
            PQLColumn(name=data_source_attribute.name(i), query=data_source_attribute.pql)
            for i, data_source_attribute in enumerate(data_source_attributes)
        ]

    def get_filters(self) -> List[PQLFilter]:
        """Returns filters of component.

        Returns:
            Component filters.
        """
        return [
            filter_.to_pql()
            for data_source in self.settings.data_sources
            if data_source is not None
            for filter_ in data_source.filters
            if filter_ is not None
        ]

    def get_order_by_columns(self) -> List[OrderByColumn]:
        """Returns order by columns of component.

        Returns:
            Order by columns of component
        """
        return []


class KpiList(DataComponent):
    """Class for KPI List components."""

    TYPE: ClassVar[str] = "kpi-list"


class KpiCard(DataComponent):
    """Class for Kpi Card components."""

    TYPE: ClassVar[str] = "kpi-card"


class Table(DataComponent):
    """Class for table components."""

    TYPE: ClassVar[str] = "table"

    def get_order_by_columns(self) -> List[OrderByColumn]:
        """Returns order by columns of component.

        Returns:
            Order by columns of component.
        """
        # Lookup table for retrieving attributes by field/id
        data_source_attributes_dict = {
            attribute.id: attribute
            for data_source in self.settings.data_sources
            for attribute in data_source.attributes
        }

        order_by_columns = {
            raw_column.order: OrderByColumn(
                query=data_source_attributes_dict[raw_column.field].pql,
                ascending=raw_column.direction == "ASC",
            )
            for raw_column in self.settings.data.sort_by  # type: ignore
        }
        return [column for _, column in sorted(order_by_columns.items())]  # Return list sorted by `order`


class Waterfall(DataComponent):
    """Class for Waterfall charts."""

    TYPE: ClassVar[str] = "waterfall"


class BarChart(DataComponent):
    """Class for Bar charts."""

    TYPE: ClassVar[str] = "bar-chart"


class PieChart(DataComponent):
    """Class for Pie charts."""

    TYPE: ClassVar[str] = "pie-chart"


class Treemap(DataComponent):
    """Class for Treemap charts."""

    TYPE: ClassVar[str] = "treemap"


class StackedBarChart(DataComponent):
    """Class for Stacked Bar charts."""

    TYPE: ClassVar[str] = "stacked-bar-chart"


class GroupedBarChart(DataComponent):
    """Class for Grouped Bar charts."""

    TYPE: ClassVar[str] = "grouped-bar-chart"


class BarPointsChart(DataComponent):
    """Class for Bar Points charts."""

    TYPE: ClassVar[str] = "bar-points-chart"


class Scatterplot(DataComponent):
    """Class for Scatterplot charts."""

    TYPE: ClassVar[str] = "scatterplot"


class LineChart(DataComponent):
    """Class for Line charts."""

    TYPE: ClassVar[str] = "line-chart"


class ColumnsChart(DataComponent):
    """Class for Columns charts."""

    TYPE: ClassVar[str] = "columns-chart"


class GroupedColumnsChart(DataComponent):
    """Class for Grouped Columns charts."""

    TYPE: ClassVar[str] = "grouped-columns-chart"


class ColumnsLineChart(DataComponent):
    """Class for Columns Line charts."""

    TYPE: ClassVar[str] = "columns-line-chart"


class StackedColumnsChart(DataComponent):
    """Class for Stacked Columns charts."""

    TYPE: ClassVar[str] = "stacked-columns-chart"


class Histogram(DataComponent):
    """Class for Histogram charts."""

    TYPE: ClassVar[str] = "histogram"


class WorldMap(DataComponent):
    """Class for World Map charts."""

    TYPE: ClassVar[str] = "world-map"


class Sankey(DataComponent):
    """Class for Sankey charts."""

    TYPE: ClassVar[str] = "sankey"
