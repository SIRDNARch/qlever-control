import os
from typing import Dict, Any, Tuple, List


class Config:
    """Configuration class for SPARQL test suite execution."""

    def __init__(self,
                 port: str,
                 graph_store: str,
                 type_alias: List[Tuple[str, str]],
                 testsuite_dir: str,
                 binaries_directory: str,
                 exclude: str,
                 ):
        self.server_address = 'localhost'
        self.port = port
        self.GRAPHSTORE = graph_store
        self.alias = type_alias
        self.path_to_test_suite = os.path.abspath(testsuite_dir)
        self.path_to_binaries = os.path.abspath(binaries_directory)
        self.exclude = exclude
        self.number_types = [
            "http://www.w3.org/2001/XMLSchema#integer",
            "http://www.w3.org/2001/XMLSchema#double",
            "http://www.w3.org/2001/XMLSchema#decimal",
            "http://www.w3.org/2001/XMLSchema#float",
            "http://www.w3.org/2001/XMLSchema#int",
            "http://www.w3.org/2001/XMLSchema#decimal"
        ]

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary format."""
        return self.__dict__
