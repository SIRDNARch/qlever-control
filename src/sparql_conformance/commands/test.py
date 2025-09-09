from qlever.command import QleverCommand
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.extract_tests import extract_tests
from sparql_conformance.testsuite import TestSuite

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

    def get_engine_manager(self, engine_type: str) -> EngineManager:
        """Get the appropriate engine manager for the given engine type"""
        managers = {
            'qlever-binaries': QLeverBinaryManager,
            'qlever': QLeverManager,
            'mdb': MDBManager,
            'oxigraph': OxigraphManager
        }

        manager_class = managers.get(engine_type)
        if manager_class is None:
            raise ValueError(f"Unsupported engine type: {engine_type}")

        return manager_class()

    def description(self) -> str:
        return "Run SPARQL conformance tests against different engines"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {
            "conformance": ["name", "port", "engine", "graph_store",
                            "testsuite_dir", "type_alias"],
            "runtime": ["system"]
        }

    def additional_arguments(self, subparser):
        pass

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
        for arg in args.__dict__:
            print(arg)
        print(args.name)
        print(args.engine)
        print(args.type_alias)
        return True
        config = initialize_config()
        if config is None:
            return False
        tests, test_count = extract_tests(config)
        test_suite = TestSuite(name=args.name, tests=tests, test_count=test_count, config=config,
                               engine_type=args.engine)
        test_suite.run()
        test_suite.generate_json_file()
        return True
