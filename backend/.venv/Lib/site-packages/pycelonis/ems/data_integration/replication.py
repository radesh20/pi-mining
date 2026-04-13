"""Module to interact with replications.

This module contains class to interact with a replication in EMS data integration.

Typical usage example:

```python
replication = data_connection.get_replication(
    "ID"
)

replication.initialize(
    extraction=True,
    transformation=True,
)
replication.start()
replication.stop()
replication.delete()
```
"""

import logging
import typing
from typing import Optional, List

from pycelonis.service.replication.service import (
    ReplicationTransport,
    ReplicationService,
    InitializationSelectionTransport,
    ReplicationInitializationSelectionTransport,
)
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


logger = logging.getLogger(__name__)


class Replication(ReplicationTransport):
    """Replication object to interact with replication specific data integration endpoints."""

    client: Client = Field(..., exclude=True)
    id: str
    """Id of replication."""
    data_pool_id: str
    """Id of data pool where replication is located."""
    table_name: str
    """Name of table."""

    @classmethod
    def from_transport(cls, client: Client, replication_transport: ReplicationTransport) -> "Replication":
        """Creates high-level replication object from the given ReplicationTransport.

        Args:
            client: Client to use to make API calls for given job.
            replication_transport: ReplicationTransport object containing properties of replication.

        Returns:
            A Replication object with properties from transport and given client.
        """
        return cls(client=client, **replication_transport.dict())

    def sync(self) -> None:
        """Syncs replication properties with EMS."""
        synced_replication = ReplicationService.get_api_pools_pool_id_replications_replication_id(
            self.client, self.data_pool_id, self.id
        )
        self._update(synced_replication)

    def delete(self) -> None:
        """Deletes replication."""
        ReplicationService.delete_api_pools_pool_id_replications_replication_id(self.client, self.data_pool_id, self.id)
        logger.info("Successfully deleted replication with id '%s'", self.id)

    def initialize(
        self,
        extraction: bool = True,
        transformation: bool = True,
        initialization_script_ids: Optional[List[str]] = None,
    ) -> None:
        """Initializes replication which triggers complete reload of data.

        Args:
            extraction: Whether to select extraction for initialization.
            transformation: Whether to select transformation for initialization.
            initialization_script_ids: Optional list of initialization script ids to use for initialization.
        """
        ReplicationService.post_api_pools_pool_id_replications_initialize(
            self.client,
            self.data_pool_id,
            InitializationSelectionTransport(
                initialization_script_ids=initialization_script_ids or [],
                replication_initialization_selections=[
                    ReplicationInitializationSelectionTransport(
                        replication_id=self.id,
                        extraction_selected=extraction,
                        transformation_selected=transformation,
                    )
                ],
            ),
        )
        logger.info("Successfully initialized replication with id '%s'", self.id)

    def start(self) -> None:
        """Starts replication."""
        ReplicationService.post_api_pools_pool_id_replications_replication_id_start(
            self.client, self.data_pool_id, self.id
        )
        logger.info("Successfully started replication with id '%s'", self.id)

    def stop(self) -> None:
        """Stops replication."""
        ReplicationService.post_api_pools_pool_id_replications_replication_id_stop(
            self.client, self.data_pool_id, self.id
        )
        logger.info("Successfully stopped replication with id '%s'", self.id)

    def __main_attributes__(self) -> typing.Optional[typing.List[str]]:
        return ["id", "name", "data_pool_id", "table_name", "status"]
