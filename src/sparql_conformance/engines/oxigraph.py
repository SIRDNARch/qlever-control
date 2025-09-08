import os
from argparse import Namespace
from pathlib import Path
from typing import List, Tuple

from qlever.log import mute_log
from qlever.util import run_command
from qoxigraph.commands.index import IndexCommand
from qoxigraph.commands.query import QueryCommand
from qoxigraph.commands.start import StartCommand
from qoxigraph.commands.stop import StopCommand
from sparql_conformance import util
from sparql_conformance.engines.manager import EngineManager
from sparql_conformance.models import Config
from sparql_conformance.rdf_tools import write_ttl_file, rdf_xml_to_turtle, delete_ttl_file


class OxigraphManager(EngineManager):
    def protocol_endpoint(self) -> str:
        return "query"

    def update(self, config: Config, query: str) -> Tuple[int, str]:
        return self._query(config, query, "ru", "json")

    def query(self, config: Config, query: str, result_format: str) -> Tuple[int, str]:
        return self._query(config, query, "rq", result_format)

    def _query(self, config: Config, query: str, query_type: str, result_format: str) -> Tuple[int, str]:
        endpoint = "query" if query_type == "rq" else "update"
        content_type = endpoint + "="
        args = Namespace(
            query=query,
            host_name=config.server_address,
            port=config.port,
            sparql_endpoint=f"localhost:{config.port}/{endpoint}",
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

    def setup(
            self,
            config: Config,
            graph_paths: Tuple[Tuple[str, str], ...]
    ) -> Tuple[bool, bool, str, str]:
        """
        Prepare Oxigraph for testing:
        - Convert .rdf to .ttl when needed (named graphs ignored by Oxigraph).
        - Copy input files into CWD (mounted into the container).
        - Build the index.
        - Start the server.
        """
        server_success = False
        graphs: List[Tuple[str, str]] = []


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

    def cleanup(self, config: Config):
        """
        Stop the Oxigraph server and remove local files created during tests.
        """
        try:
            self._stop_server(config.port)
        except Exception:
            pass

        with mute_log():
            try:
                for p in Path.cwd().glob("*.sst"):
                    try:
                        p.unlink()
                    except Exception:
                        pass

                for pattern in [
                    "MANIFEST-*",
                    "OPTIONS-*",
                    "CURRENT",
                    "IDENTITY",
                    "LOCK",
                    "LOG",
                    "LOG.old.*",
                    "[0-9][0-9][0-9][0-9][0-9][0-9].log",
                    "[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*"
                ]:
                    for p in Path.cwd().glob(pattern):
                        try:
                            p.unlink()
                        except Exception:
                            pass
            except Exception:
                pass

            try:
                run_command("rm -f sparql-conformance-index.*")
            except Exception:
                pass

    def _index(self, graph_paths: List[Tuple[str, str]]) -> Tuple[bool, str]:
        """
        Build the Oxigraph index for the given input files.
        """
        input_files = " ".join(path for path, _ in graph_paths)

        args = Namespace(
            name="sparql-conformance-index",
            input_files=input_files,
            system="docker",
            image="ghcr.io/oxigraph/oxigraph",
            index_container="sparql-conformance-index-container",
            show=False,
        )

        try:
            with mute_log(50):
                result = IndexCommand().execute(args, True)
        except Exception as e:
            return False, str(e)

        log_path = "./sparql-conformance-index.index-log.txt"
        index_log = util.read_file(log_path) if os.path.exists(log_path) else ""

        return result, index_log

    def _start_server(self, host: str, port: int) -> Tuple[bool, str]:
        """
        Start the Oxigraph server.
        """
        args = Namespace(
            name="sparql-conformance-server",
            host_name=host,
            port=port,
            system="docker",
            image="ghcr.io/oxigraph/oxigraph",
            server_container="sparql-conformance-server",
            run_in_foreground=False,
            show=False,
        )

        try:
            with mute_log(50):
                result = StartCommand().execute(args, True)
        except Exception as e:
            return False, str(e)

        log_path = "./sparql-conformance-index.server-log.txt"
        server_log = util.read_file(log_path) if os.path.exists(log_path) else ""

        return result, server_log

    def _stop_server(self, port) -> Tuple[bool, str]:
        """
        Stop the Oxigraph server listening on the given port.
        """
        args = Namespace(
            system="docker",
            server_container="sparql-conformance-server",
            port=port,
            show=False,
            cmdline_regex=StopCommand.DEFAULT_REGEX,
        )

        try:
            with mute_log():
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)

        return result, "Success"
