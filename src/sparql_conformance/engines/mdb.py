import json
import os
import shutil
from argparse import Namespace
from pathlib import Path
from typing import Tuple, List

from qlever.log import mute_log, log
from qlever.util import run_command
from qmdb.commands.index import IndexCommand
from qmdb.commands.query import QueryCommand
from qmdb.commands.start import StartCommand
from qmdb.commands.stop import StopCommand
from sparql_conformance import util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import write_ttl_file, rdf_xml_to_turtle, delete_ttl_file


class MDBManager(EngineManager):
    """Manager for MillenniumDB using docker execution"""

    def __init__(self):
        self.first_setup: bool = True

    def query(self, config: Config, query: str, result_format: str) -> Tuple[int, str]:
        return self._query(config, query, "rq", result_format)

    def update(self, config: Config, query: str) -> Tuple[int, str]:
        return self._query(config, query, "ru", "json")

    def protocol_endpoint(self) -> str:
        return "sparql"

    def cleanup(self, config: Config):
        """
        Stop the mdb server and remove local files created during tests.
        """
        try:
            self._stop_server(config.port)
        except Exception:
            pass

        with mute_log():
            # Remove the MillenniumDB index directory
            try:
                if os.path.isdir("index"):
                    shutil.rmtree("index", ignore_errors=True)
            except Exception:
                pass

            try:
                run_command("rm -f sparql-conformance-index.index-log.txt")
            except Exception:
                pass
            try:
                run_command("rm -f conformance.server-log.txt")
            except Exception:
                pass

    def _query(self, config: Config, query: str, query_type: str, result_format: str) -> Tuple[int, str]:
        content_type = "query=" if query_type == "rq" else "update="
        args = Namespace(
            query=query,
            host_name=config.server_address,
            port=config.port,
            sparql_endpoint=None,
            accept=util.get_accept_header(result_format),
            pin_to_cache=False,
            no_time=True,
            predefined_query=None,
            show=False,
            log_level="ERROR",
            content_type=content_type,
        )

        try:
            with mute_log():
                qc = QueryCommand()
                success = qc.execute(args, True)
                body, _, status_line = qc.query_output.rpartition("HTTP_STATUS:")
                status = int(status_line.strip())
            return status, body
        except Exception as e:
            return 1, str(e)

    def setup(self, config: Config, graph_paths: Tuple[Tuple[str, str], ...]) -> Tuple[bool, bool, str, str]:
        """
        Prepare MillenniumDB for testing:
        - Convert .rdf to .ttl when needed (named graphs ignored by mdb).
        - Copy input files into CWD (mounted into the container).
        - Build the index.
        - Start the server.
        """
        if self.first_setup:
            log.info("This is the first setup.")
            self.first_setup = False
            success, message = self._setup_docker_image(graph_paths[0][0])
            if not success:
                self.cleanup(config)
                return success, False, message, ""
            self.cleanup(config)

        server_success = False
        graphs: List[Tuple[str, str]] = []
        # Normalize inputs: convert .rdf -> .ttl and copy files into the workdir
        for graph_path, graph_name in graph_paths:
            if graph_path.endswith(".rdf"):
                tmp_name = Path(graph_path).name.replace(".rdf", ".ttl")
                write_ttl_file(tmp_name, rdf_xml_to_turtle(graph_path, graph_name))
                use_path = tmp_name
            else:
                use_path = util.copy_graph_to_workdir(graph_path, os.getcwd())
            graphs.append((use_path, graph_name))

        index_success, index_log = self._index(graphs)
        if not index_success:
            for path, _ in graphs:
                try:
                    delete_ttl_file(path)
                except Exception:
                    pass
            return index_success, server_success, index_log, ""

        server_success, server_log = self._start_server(
            config.server_address,
            config.port
        )
        for path, _ in graphs:
            try:
                delete_ttl_file(path)
            except Exception:
                pass

        return index_success, server_success, index_log, server_log

    def _stop_server(self, port: str) -> Tuple[bool, str]:
        """
        Stop the MillenniumDB (mdb) server listening on the given port.
        """
        args = Namespace(
            port=port,
            server_container="sparql-conformance-server",
            system="docker",
            cmdline_regex=StopCommand.DEFAULT_REGEX,
            show=False,
        )

        try:
            with mute_log():
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)

        return result, "Success"

    def _start_server(self, host: str, port: int) -> Tuple[bool, str]:
        """
        Start the MillenniumDB (mdb) server.

        Returns:
            (success, server_log)
        """
        args = Namespace(
            name="sparql-conformance-server",
            host_name=host,
            port=port,
            system="docker",
            image="millenniumdb-image",
            server_container="sparql-conformance-server",
            run_in_foreground=False,
            server_binary="mdb server",
            show=False,
        )

        try:
            with mute_log(50):
                result = StartCommand().execute(args, True)
        except Exception as e:
            return False, str(e)

        log_path = "./sparql-conformance-server.server-log.txt"
        server_log = util.read_file(log_path) if os.path.exists(log_path) else ""

        return result, server_log

    def _index(self, graph_paths: List[Tuple[str, str]]) -> Tuple[bool, str]:
        """
        Build the MillenniumDB (mdb) index for the given graphs.

        Returns:
            (success, index_log)
        """
        # MillenniumDB importer just takes input files
        input_files = " ".join(path for path, _ in graph_paths) if graph_paths else "*.ttl"

        args = Namespace(
            name="sparql-conformance-index",
            input_files=input_files,
            system="docker",
            image="millenniumdb-image",
            index_container="sparql-conformance-container",
            index_binary="mdb import",
            show=False,
        )

        try:
            with mute_log():
                result = IndexCommand().execute(args, True)
        except Exception as e:
            return False, str(e)

        index_log = ""
        log_path = "./sparql-conformance-index.index-log.txt"
        if os.path.exists(log_path):
            index_log = util.read_file(log_path)

        return result, index_log

    def _generate_multi_input_json(self, graph_paths: List[Tuple[str, str]]) -> str:
        """Generate the JSON input for multi_input_json in IndexCommand.execute()"""
        input_list = []
        for graph_path, graph_name in graph_paths:
            entry = {
                'cmd': f'cat {graph_path}',
                'graph': graph_name if graph_name else '-',
                'format': 'ttl'
            }
            input_list.append(entry)
        return json.dumps(input_list)

    def _setup_docker_image(self, graph_path: str) -> Tuple[bool, str]:
        """
        Build the MillenniumDB image which will be used.

        Returns:
            (success, index_log)
        """
        args = Namespace(
            name="sparql-conformance-index",
            input_files=graph_path,
            system="docker",
            image="millenniumdb-image",
            index_container="sparql-conformance-container",
            index_binary="mdb import",
            show=False,
        )
        log.info("Building the docker image...")
        try:
            with mute_log(50):
                result = IndexCommand().execute(args, True)
            return result, ""
        except Exception as e:
            return False, str(e)
