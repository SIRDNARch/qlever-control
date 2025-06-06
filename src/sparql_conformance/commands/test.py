from qlever.command import QleverCommand
from qlever.commands.index import IndexCommand
from sparql_conformance.engines.qlever import QLeverManager
from sparql_conformance.testsuite import TestSuite
from sparql_conformance.config_manager import initialize_config
from sparql_conformance.extract_tests import extract_tests

class TestCommand(QleverCommand):
    """
    Class for executing the `test` command.
    """

    def __init__(self):
        self.options = [
            'qlever',
            'qlever-binaries',
            'qmdb'
        ]
        pass

    def description(self) -> str:
        return "Run SPARQL conformance tests against different engines"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {}

    def additional_arguments(self, subparser):
        subparser.add_argument(
            '--name',
            required=True,
            help='Name of conformance test run'
        )
        subparser.add_argument(
            '--engine',
            choices=list(self.options),
            required=True,
            help='SPARQL engine to test'
        )
        subparser.add_argument(
            '--test-suite-path',
            required=True,
            help='Path to W3C SPARQL test suite'
        )
        subparser.add_argument(
            '--test-type',
            choices=['query', 'update', 'syntax', 'protocol'],
            default='all',
            help='Type of conformance tests to run'
        )

    def execute(self, args) -> bool:
#         test = (('/Users/ricoandris/Desktop/master-project/conformance/rdf-tests/sparql/sparql11/csv-tsv-res/data.ttl', '-'),)#,
#          #('/Users/ricoandris/Desktop/master-project/conformance/rdf-tests/sparql/sparql11/csv-tsv-res/data2.tt',
#           #'data2'))
#         query = '''PREFIX : <http://example.org/>
#
# SELECT * WHERE { ?s ?p ?o} ORDER BY ?s ?p ?o'''
#         config = initialize_config()
#         m = QLeverManager()
#         m.setup(config, test)
#         m.query(config,query,'rq','csv')
#         m.cleanup(config)
        config = initialize_config()
        if config is None:
            return False
        tests, test_count = extract_tests(config)
        test_suite = TestSuite(name=args.name, tests=tests, test_count=test_count, config=config,
                               engine_type=args.engine)
        test_suite.run()
        test_suite.generate_json_file()
        return True
