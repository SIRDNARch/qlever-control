from qlever.command import QleverCommand
from sparql_conformance.config_manager import create_config

class CreateConfigCommand(QleverCommand):
    """
    Command for creating a SPARQL conformance test configuration
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Create configuration for SPARQL conformance testing"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {}

    def additional_arguments(self, subparser):
        subparser.add_argument(
            '--server-address',
            required=True,
            help='Server address for the SPARQL endpoint'
        )
        subparser.add_argument(
            '--port',
            required=True,
            help='Port number for the SPARQL endpoint'
        )
        subparser.add_argument(
            '--testsuite-path',
            required=True,
            help='Path to W3C SPARQL test suite'
        )
        subparser.add_argument(
            '--binaries-path',
            required=True,
            help='Path to the QLever binaries'
        )
        subparser.add_argument(
            '--host',
            required=True,
            help='Graph store implementation host'
        )
        subparser.add_argument(
            '--graphstore',
            required=True,
            help='Path of the URL of the graph store'
        )
        subparser.add_argument(
            '--newpath',
            required=True,
            help='URL returned in the Location HTTP header'
        )

    def execute(self, args) -> bool:
        print("Creating SPARQL conformance test configuration...")

        success = create_config(
            server_address=args.server_address,
            port=args.port,
            path_to_testsuite=args.testsuite_path,
            path_to_binaries=args.binaries_path,
            host=args.host,
            graphstore=args.graphstore,
            newpath=args.newpath
        )

        if success:
            print("Configuration created successfully!")
            return True
        else:
            print("Failed to create configuration. Please check the paths and try again.")
            return False