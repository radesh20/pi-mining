"""Module for view content."""

from typing import List

from pycelonis.ems.apps.content_node.view.component import Component
from pycelonis.ems.apps.content_node.view.component_factory import ComponentFactory
from pycelonis_core.base.base_model import PyCelonisBaseModel
from pycelonis_core.base.collection import CelonisCollection

try:
    from pydantic.v1 import Extra, Field, validator  # type: ignore
except ImportError:
    from pydantic import Extra, Field, validator  # type: ignore


class ViewMetadata(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for basic view metadata."""

    key: str
    """Key of view."""
    name: str
    """Name of view."""
    knowledge_model_key: str
    """Key of knowledge model used by view."""

    def __main_attributes__(self) -> List[str]:
        return ["key", "name"]


class ViewTab(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view tab containing components such as tables or kpis."""

    id: str
    """Id of view tab."""
    name: str
    """Name of view tab."""
    components: CelonisCollection[Component] = Field(default_factory=CelonisCollection)
    """Components (tables, kpis, etc.) of view tab."""

    # validators
    _components_validator = validator("components", allow_reuse=True)(ComponentFactory.get_components)

    def __main_attributes__(self) -> List[str]:
        return ["id", "name"]


class ViewContent(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for view content to read properties from view."""

    metadata: "ViewMetadata"
    """Metadata of view."""

    components: CelonisCollection[Component] = Field(default_factory=CelonisCollection)

    tabs: CelonisCollection[ViewTab] = Field(default_factory=CelonisCollection)

    # validators
    _components_validator = validator("components", allow_reuse=True)(ComponentFactory.get_components)
    _tabs_validator = validator("tabs", allow_reuse=True)(CelonisCollection.from_list)
