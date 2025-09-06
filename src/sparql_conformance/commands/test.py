from qlever.command import QleverCommand
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
            'mdb',
            'oxigraph'
        ]

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

    def execute(self, args) -> bool:
#         test = (('/Users/ricoandris/Desktop/master-project/conformance/rdf-tests/sparql/sparql11/csv-tsv-res/data.ttl', '-'),
#                 ('/Users/ricoandris/Desktop/master-project/conformance/rdf-tests/sparql/sparql11/csv-tsv-res/data2.ttl','data2'))
#         query = '''PREFIX : <http://example.org/>
#
# SELECT * WHERE { ?s ?p ?o} ORDER BY ?s ?p ?o'''
#         config = initialize_config()
#         m = QMDBManager()
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
