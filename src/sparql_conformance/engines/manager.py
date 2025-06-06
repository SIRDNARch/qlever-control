from abc import ABC, abstractmethod
from typing import Tuple


class EngineManager(ABC):
    """Abstract base class for SPARQL engine managers"""

    @abstractmethod
    def index(self, command_index: str, graph_paths: list) -> Tuple[bool, str]:
        """Index the given graph files"""
        pass

    @abstractmethod
    def remove_index(self, command_remove_index: str) -> Tuple[bool, str]:
        """Remove index files"""
        pass

    @abstractmethod
    def start_server(self, command_start_server: str, server_address: str, port: str) -> Tuple[int, str]:
        """Start the SPARQL server"""
        pass

    @abstractmethod
    def stop_server(self, command_stop_server: str) -> str:
        """Stop the SPARQL server"""
        pass

    @abstractmethod
    def query(self, query: str, query_type: str, result_format: str,
              server_address: str, port: str) -> Tuple[int, str]:
        """Execute a SPARQL query"""
        pass
