from typing import List

from pycelonis.ems.apps.content_node.view.component import (
    Component,
    Table,
    Waterfall,
    BarChart,
    PieChart,
    Treemap,
    StackedBarChart,
    GroupedBarChart,
    BarPointsChart,
    Scatterplot,
    LineChart,
    ColumnsChart,
    GroupedColumnsChart,
    ColumnsLineChart,
    StackedColumnsChart,
    Histogram,
    WorldMap,
    Sankey,
    KpiCard,
    KpiList,
)
from pycelonis_core.base.collection import CelonisCollection


class ComponentFactory:
    """Factory class to create Component with given type."""

    @staticmethod
    def get_component(component: Component) -> "Component":
        """Returns instance of Component depending on type.

        Args:
            component: Component containing properties of component.

        Returns:
            A Component object with proper class based on type.
        """
        if component.type_ == Table.TYPE:
            return Table(**component.dict())
        if component.type_ == Waterfall.TYPE:
            return Waterfall(**component.dict())
        if component.type_ == BarChart.TYPE:
            return BarChart(**component.dict())
        if component.type_ == PieChart.TYPE:
            return PieChart(**component.dict())
        if component.type_ == Treemap.TYPE:
            return Treemap(**component.dict())
        if component.type_ == StackedBarChart.TYPE:
            return StackedBarChart(**component.dict())
        if component.type_ == GroupedBarChart.TYPE:
            return GroupedBarChart(**component.dict())
        if component.type_ == BarPointsChart.TYPE:
            return BarPointsChart(**component.dict())
        if component.type_ == Scatterplot.TYPE:
            return Scatterplot(**component.dict())
        if component.type_ == LineChart.TYPE:
            return LineChart(**component.dict())
        if component.type_ == ColumnsChart.TYPE:
            return ColumnsChart(**component.dict())
        if component.type_ == GroupedColumnsChart.TYPE:
            return GroupedColumnsChart(**component.dict())
        if component.type_ == ColumnsLineChart.TYPE:
            return ColumnsLineChart(**component.dict())
        if component.type_ == StackedColumnsChart.TYPE:
            return StackedColumnsChart(**component.dict())
        if component.type_ == Histogram.TYPE:
            return Histogram(**component.dict())
        if component.type_ == WorldMap.TYPE:
            return WorldMap(**component.dict())
        if component.type_ == Sankey.TYPE:
            return Sankey(**component.dict())
        if component.type_ == KpiCard.TYPE:
            return KpiCard(**component.dict())
        if component.type_ == KpiList.TYPE:
            return KpiList(**component.dict())

        return Component(**component.dict())

    @staticmethod
    def get_components(components: List[Component]) -> CelonisCollection[Component]:
        """Returns instances of Component depending on type.

        Args:
            components: List of Component containing properties of components.

        Returns:
            A CelonisCollection of Component objects with proper class based on type.
        """
        return CelonisCollection(ComponentFactory.get_component(component) for component in components)
