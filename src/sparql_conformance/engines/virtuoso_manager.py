from __future__ import annotations

import os
from pathlib import Path

from qlever.log import mute_log
from qlever.util import run_command
from qvirtuoso.commands.index import IndexCommand
from qvirtuoso.commands.query import QueryCommand
from qvirtuoso.commands.start import StartCommand
from qvirtuoso.commands.stop import StopCommand
from sparql_conformance import util as conformance_util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager
from sparql_conformance.rdf_tools import rdf_xml_to_turtle, write_ttl_file


DEFAULT_NAME = "qlever-sparql-conformance"
DEFAULT_GRAPH_URI = "urn:qlever:default-graph"


def _make_args(config: Config, **overrides):
    return getattr(conformance_util, "make_args")(config, **overrides)


def _get_accept_header(result_format: str) -> str:
    return getattr(conformance_util, "get_accept_header")(result_format)


def _read_file(path: str) -> str:
    return getattr(conformance_util, "read_file")(path)


def _copy_graph_to_workdir(file_path: str, workdir: str) -> str:
    return getattr(conformance_util, "copy_graph_to_workdir")(
        file_path, workdir
    )


class VirtuosoManager(EngineManager):
    """Manager for Virtuoso using qvirtuoso commands."""

    def protocol_endpoint(self) -> str:
        return "sparql"

    def setup(
        self,
        config: Config,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[bool, bool, str, str]:
        server_success = False
        graph_files, cleanup_paths, graph_names = self._prepare_graphs(
            graph_paths
        )
        index_success, index_log = self._index(
            config, graph_files, graph_names
        )
        self._cleanup_graph_copies(cleanup_paths)
        if not index_success:
            return index_success, server_success, index_log, ""

        server_success, server_log = self._start_server(config)
        if not server_success:
            return index_success, server_success, index_log, server_log
        return index_success, server_success, index_log, server_log

    def cleanup(self, config: Config):
        self._stop_server(config)
        self._stop_index_container(config)
        with mute_log():
            run_command(
                f"rm -f {DEFAULT_NAME}.index-log.txt "
                f"{DEFAULT_NAME}.server-log.txt "
                "virtuoso.db virtuoso.trx virtuoso.pxa"
            )

    def query(
        self,
        config: Config,
        query: str,
        result_format: str,
    ) -> tuple[int, str]:
        return self._query(config, query, "query=", result_format)

    def update(self, config: Config, query: str) -> tuple[int, str]:
        return self._query(config, query, "update=", "json")

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
    ) -> tuple[int, str]:
        args = _make_args(
            config,
            accept=_get_accept_header(result_format),
            query=query,
            content_type=content_type,
        )
        args.default_graph_uri = DEFAULT_GRAPH_URI
        try:
            with mute_log():
                qc = QueryCommand()
                qc.execute(args, called_from_conformance_test=True)
                query_output = str(qc.query_output)
                body, _, status_line = query_output.rpartition("HTTP_STATUS:")
                status_line = status_line.strip()
                if not status_line:
                    return 1, query_output
                status = int(status_line)
            return status, body
        except Exception as e:
            return 1, str(e)

    def _index(
        self,
        config: Config,
        graph_files: list[str],
        graph_names: list[str],
    ) -> tuple[bool, str]:
        index_binary = "isql"
        server_binary = "virtuoso-t"
        if config.system == "native":
            index_binary = str(Path(config.path_to_binaries, index_binary))
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            index_binary=index_binary,
            isql_port=1111,
            num_parallel_loaders=1,
            free_memory_gb="4G",
            server_binary=server_binary,
        )
        args.graph_files = graph_files
        args.graph_names = graph_names
        args.default_graph_uri = DEFAULT_GRAPH_URI
        try:
            with mute_log():
                result = IndexCommand().execute(
                    args=args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        index_log = _read_file(f"./{DEFAULT_NAME}.index-log.txt")
        return result, index_log

    def _start_server(self, config: Config) -> tuple[bool, str]:
        server_binary = "virtuoso-t"
        if config.system == "native":
            server_binary = str(Path(config.path_to_binaries, server_binary))
        args = _make_args(
            config,
            server_binary=server_binary,
            max_query_memory="2G",
            extra_args="",
            run_in_foreground=False,
            timeout="30s",
        )
        args.default_graph_uri = DEFAULT_GRAPH_URI
        try:
            with mute_log():
                result = StartCommand().execute(
                    args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        server_log = _read_file(f"./{DEFAULT_NAME}.server-log.txt")
        return result, server_log

    def _stop_server(self, config: Config) -> tuple[bool, str]:
        args = _make_args(config, cmdline_regex=StopCommand.DEFAULT_REGEX)
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)
        return result, "Success"

    def _stop_index_container(self, config: Config) -> tuple[bool, str]:
        args = _make_args(config)
        args.server_container = args.index_container
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)
        return result, "Success"

    def _prepare_graphs(
        self,
        graph_paths: tuple[tuple[str, str], ...],
    ) -> tuple[list[str], list[Path], list[str]]:
        workdir = Path(os.getcwd()).resolve()
        graph_files: list[str] = []
        cleanup_paths: list[Path] = []
        graph_names: list[str] = []
        for graph_path, graph_name in graph_paths:
            if graph_path.endswith(".rdf"):
                graph_path_new = Path(graph_path).name
                graph_path_new = graph_path_new.replace(".rdf", ".ttl")
                write_ttl_file(
                    graph_path_new,
                    rdf_xml_to_turtle(graph_path, graph_name),
                )
                graph_files.append(graph_path_new)
                cleanup_paths.append(workdir / graph_path_new)
                graph_names.append(self._map_graph_name(graph_name))
                continue
            src = Path(graph_path).resolve()
            if src.parent == workdir:
                graph_files.append(src.name)
                graph_names.append(self._map_graph_name(graph_name))
                continue
            graph_files.append(
                _copy_graph_to_workdir(str(src), str(workdir))
            )
            cleanup_paths.append(workdir / src.name)
            graph_names.append(self._map_graph_name(graph_name))
        return graph_files, cleanup_paths, graph_names

    @staticmethod
    def _map_graph_name(graph_name: str) -> str:
        if graph_name == "-":
            return DEFAULT_GRAPH_URI
        return graph_name

    def _cleanup_graph_copies(self, cleanup_paths: list[Path]) -> None:
        for path in cleanup_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                continue
