"""Module to interact with data connections.

This module contains class to interact with a data connection in EMS data integration.

Typical usage example:

```python
data_connection = data_pool.get_data_connection(
    "ID"
)

data_connection.sync()
data_connection.delete()
```
"""

import logging
import typing

from pycelonis.ems.data_integration.replication import Replication
from pycelonis.service.integration.service import DataSourceTable, DataSourceTransport, IntegrationService
from pycelonis.service.replication.service import ReplicationService, ReplicationTransport
from pycelonis_core.base.collection import CelonisCollection
from pycelonis_core.client.client import Client
from pycelonis_core.utils.errors import PyCelonisValueError

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


logger = logging.getLogger(__name__)


class DataConnection(DataSourceTransport):
    """DataConnection object to interact with data connection specific data integration endpoints."""

    client: Client = Field(..., exclude=True)
    id: str
    """Id of data connection."""
    pool_id: str
    """Id of pool where data connection is located."""
    name: typing.Optional[str]
    """Name of data connection."""

    @classmethod
    def from_transport(cls, client: Client, data_source_transport: DataSourceTransport) -> "DataConnection":
        """Creates high-level data connection object from the given DataSourceTransport.

        Args:
            client: Client to use to make API calls for given job.
            data_source_transport: DataSourceTransport object containing properties of data connection.

        Returns:
            A DataConnection object with properties from transport and given client.
        """
        return cls(client=client, **data_source_transport.dict())

    @property
    def data_pool_id(self) -> str:
        """Returns id of data pool for given data connection.

        Returns:
            Data pool id.
        """
        return self.pool_id

    @data_pool_id.setter
    def data_pool_id(self, data_pool_id: str) -> None:
        """Sets data pool id for given connection.

        Args:
            data_pool_id: New data pool id.
        """
        self.pool_id = data_pool_id

    def sync(self) -> None:
        """Syncs data connection properties with EMS."""
        synced_data_connection = IntegrationService.get_api_pools_pool_id_data_sources_data_source_id(
            self.client, self.pool_id, self.id
        )
        self._update(synced_data_connection)

    def delete(self) -> None:
        """Deletes data connection."""
        IntegrationService.delete_api_pools_pool_id_data_sources_data_source_id(self.client, self.data_pool_id, self.id)
        logger.info("Successfully deleted data connection with id '%s'", self.id)

    def __main_attributes__(self) -> typing.Optional[typing.List[str]]:
        return ["id", "name", "pool_id", "target_schema_name", "type_"]

    ############################################################
    # Table
    ############################################################
    def get_tables(self, search_string: typing.Optional[str] = None) -> typing.List[typing.Optional["DataSourceTable"]]:
        """Returns tables matching given search string. If no search string is given, all tables are returned.

        Args:
            search_string: Search string to filter tables. Default is None which returns all tables.

        Returns:
            Returns list of DataSourceTable objects containing tables matching search string.

        Raises:
            PyCelonisValueError: Raised if search for tables with given search string failed.
        """
        available_tables = IntegrationService.get_api_pools_pool_id_data_sources_data_source_id_search_tables(
            self.client,
            self.data_pool_id,
            self.id,
            search_string=search_string,
        ).available_tables

        if available_tables is None:
            raise PyCelonisValueError(f"Search for tables with search string {search_string} failed.")

        return available_tables

    ############################################################
    # Replication
    ############################################################
    def create_replication(self, table_name: str, **kwargs: typing.Any) -> "ReplicationTransport":
        """Creates new replication for table with name in given data connection.

        Args:
            table_name: Name of table for which to create replication.
            **kwargs: Additional parameters set for
                [DataModelTransport][pycelonis.service.replication.service.ReplicationTransport] object.

        Returns:
            A Replication object for newly created replication.

        Examples:
            Create a replication for table `TEST_TABLE` and initialize/start it:
            ```python
            replication = data_connection.create_replication(
                "TEST_TABLE"
            )
            replication.initialize()  # initializes and triggers replication
            ```
        """
        replication_transports = ReplicationService.post_api_pools_pool_id_replications(
            self.client,
            self.data_pool_id,
            [ReplicationTransport(data_source_id=self.id, table_name=table_name, **kwargs)],
        )

        if len(replication_transports) != 1 or replication_transports[0] is None:
            raise PyCelonisValueError(
                "Something went wrong while creating the replication. Make sure the data pool and data connection still exist."
            )

        logger.info("Successfully created replication with id '%s'", replication_transports[0].id)
        return Replication.from_transport(self.client, replication_transports[0])

    def get_replication(self, id_: str) -> "Replication":
        """Gets replication with given id.

        Args:
            id_: Id of replication.

        Returns:
            A Replication object for replication with given id.
        """
        replication_transport = ReplicationService.get_api_pools_pool_id_replications_replication_id(
            self.client, self.data_pool_id, id_
        )
        return Replication.from_transport(self.client, replication_transport)

    def get_replications(self) -> typing.List["Replication"]:
        """Gets all replications of given data connection.

        Returns:
            A list containing all replications.
        """
        replication_transports = ReplicationService.get_api_pools_pool_id_replications(
            self.client, self.data_pool_id, data_source_id=self.id
        )
        return CelonisCollection(
            Replication.from_transport(self.client, replication_transport)
            for replication_transport in replication_transports
            if replication_transport is not None
        )
