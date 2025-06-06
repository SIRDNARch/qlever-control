from abc import ABC, abstractmethod
from typing import Tuple

from sparql_conformance.models import Config


class EngineManager(ABC):
    """Abstract base class for SPARQL engine managers"""

    @abstractmethod
    def setup(self,
              config: Config,
              graph_paths: Tuple[Tuple[str, str], ...]
              ) -> Tuple[bool, bool, str, str]:
        """
        Set up the engine for testing.

        Args:
            config: Test suite config, used to set engine-specific settings
            graph_paths: ex. default graph + named graph (('graph_path', '-'), ('graph_path2', 'graph_name2'))
            list_of_tests: [Test1, Test2, ...]

        Returns:
            index_success (bool), server_success (bool), index_log (str), server_log (str)
        """
        pass

    @abstractmethod
    def cleanup(self, config: Config):
        """Clean up the test environment after testing"""
        pass

    @abstractmethod
    def query(self, config: Config, query: str, query_type: str, result_format: str) -> Tuple[int, str]:
        """
        Send a query to the engine and return the result

        Args:
            config: Test suite config, used to set engine-specific settings
            query: The SPARQL query to be executed
            query_type: Query or Update
            result_format: Type of the result

        Returns:
           HTTP status code (int), query result (str)
        """
        pass
