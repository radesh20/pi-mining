"""Module to interact with Views.

This module contains class to interact with Views in Apps.
"""

from typing import Optional

from pycelonis.ems.apps.content_node import PublishedContentNode
from pycelonis.ems.apps.content_node.view.content import ViewContent
from pycelonis.utils.json import load_json


class PublishedView(PublishedContentNode):
    """View object to interact with view specific apps endpoints."""

    def get_content(self) -> Optional["ViewContent"]:
        """Returns content of view.

        Returns:
            View content.

        Examples:
            Extract table from view content:
            ```python
            table = view.get_content().components.find_by_id(
                "<component_id>"
            )
            ```
        """
        if self.serialized_content is None:
            return None
        return ViewContent(**load_json(self.serialized_content))
