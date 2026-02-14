import os
import shutil
from pathlib import Path
from typing import Tuple, List

from qblazegraph.commands.index import IndexCommand
from qblazegraph.commands.start import StartCommand
from qblazegraph.commands.stop import StopCommand
from qlever.commands.query import QueryCommand
from qlever.log import mute_log
from qlever.util import run_command
import sparql_conformance.util as conformance_util
from sparql_conformance.config import Config
from sparql_conformance.engines.engine_manager import EngineManager


def _make_args(config: Config, **overrides):
    return getattr(conformance_util, "make_args")(config, **overrides)


def _get_accept_header(result_format: str) -> str:
    return getattr(conformance_util, "get_accept_header")(result_format)


def _read_file(path: str) -> str:
    return getattr(conformance_util, "read_file")(path)


def _copy_graph_to_workdir(file_path: str, workdir: str) -> str:
    return getattr(conformance_util, "copy_graph_to_workdir")(file_path, workdir)


class BlazegraphManager(EngineManager):
    """Manager for Blazegraph using qblazegraph commands."""

    def protocol_endpoint(self) -> str:
        return "blazegraph/namespace/kb/sparql"

    def setup(
        self,
        config: Config,
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> Tuple[bool, bool, str, str]:
        server_success = False
        try:
            self._ensure_rwstore_properties()
        except Exception as e:
            print(f"Error preparing RWStore.properties: {e}")
            return False, server_success, str(e), ""

        graph_files, cleanup_paths = self._prepare_graphs(graph_paths)
        index_success, index_log = self._index(config, graph_files)
        self._cleanup_graph_copies(cleanup_paths)
        if not index_success:
            return index_success, server_success, index_log, ""

        server_success, server_log = self._start_server(config)
        if not server_success:
            return index_success, server_success, index_log, server_log
        return index_success, server_success, index_log, server_log

    def cleanup(self, config: Config):
        self._stop_server(config)
        with mute_log():
            run_command(
                "rm -f blazegraph.jnl "
                "qlever-sparql-conformance.index-log.txt "
                "qlever-sparql-conformance.server-log.txt "
                "web.xml qlever-sparql-conformance.web.xml"
            )

    def query(
        self,
        config: Config,
        query: str,
        result_format: str,
    ) -> Tuple[int, str]:
        return self._query(config, query, "query=", result_format)

    def update(self, config: Config, query: str) -> Tuple[int, str]:
        return self._query(config, query, "update=", "json")

    def _query(
        self,
        config: Config,
        query: str,
        content_type: str,
        result_format: str,
    ) -> Tuple[int, str]:
        args = _make_args(
            config,
            accept=_get_accept_header(result_format),
            query=query,
            content_type=content_type,
            sparql_endpoint=(
                f"{config.server_address}:{config.port}"
                "/blazegraph/namespace/kb/sparql"
            ),
        )
        try:
            with mute_log():
                qc = QueryCommand()
                qc.execute(args, True)
                query_output = str(qc.query_output)
                body, _, status_line = query_output.rpartition(
                    "HTTP_STATUS:"
                )
                status = int(status_line.strip())
            return status, body
        except Exception as e:
            return 1, str(e)

    def _index(
        self,
        config: Config,
        graph_files: List[str],
    ) -> Tuple[bool, str]:
        args = _make_args(
            config,
            input_files=" ".join(graph_files),
            jvm_args="",
            extra_args="",
            blazegraph_jar="blazegraph.jar",
            image=config.image or "test",
        )
        try:
            with mute_log():
                result = IndexCommand().execute(args)
        except Exception as e:
            return False, str(e)

        index_log = _read_file(
            "./qlever-sparql-conformance.index-log.txt"
        )
        return result, index_log

    def _start_server(self, config: Config) -> Tuple[bool, str]:
        args = _make_args(
            config,
            run_in_foreground=False,
            jvm_args="",
            extra_args="",
            blazegraph_jar="blazegraph.jar",
            read_only="no",
            timeout="60s",
            image=config.image or "test",
        )
        try:
            with mute_log():
                result = StartCommand().execute(
                    args,
                    called_from_conformance_test=True,
                )
        except Exception as e:
            return False, str(e)

        server_log = _read_file(
            "./qlever-sparql-conformance.server-log.txt"
        )
        return result, server_log

    def _stop_server(self, config: Config) -> Tuple[bool, str]:
        args = _make_args(
            config,
            cmdline_regex=StopCommand.DEFAULT_REGEX,
        )
        try:
            with mute_log(50):
                result = StopCommand().execute(args)
        except Exception as e:
            return False, str(e)
        return result, "Success"

    def _ensure_rwstore_properties(self) -> None:
        destination = Path("RWStore.properties")
        if destination.exists():
            return
        repo_root = Path(__file__).resolve().parents[3]
        source = repo_root / "src" / "qblazegraph" / "RWStore.properties"
        shutil.copy(source, destination)

    def _prepare_graphs(
        self,
        graph_paths: Tuple[Tuple[str, str], ...],
    ) -> Tuple[List[str], List[Path]]:
        workdir = Path(os.getcwd()).resolve()
        graph_files = []
        cleanup_paths: List[Path] = []
        for graph_path, _graph_name in graph_paths:
            src = Path(graph_path).resolve()
            if src.parent == workdir:
                graph_files.append(src.name)
                continue
            graph_files.append(
                _copy_graph_to_workdir(str(src), str(workdir))
            )
            cleanup_paths.append(workdir / src.name)
        return graph_files, cleanup_paths

    def _cleanup_graph_copies(self, cleanup_paths: List[Path]) -> None:
        for path in cleanup_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                continue
