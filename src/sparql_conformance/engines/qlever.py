import json
import os
import shutil
import sys
from pathlib import Path
from argparse import Namespace
from typing import Tuple, List
import io

from qlever.commands.query import QueryCommand
from qlever.log import log, mute_log
from qlever.util import run_command
from qlever.commands.start import StartCommand
from qlever.commands.stop import StopCommand
from sparql_conformance.engines.manager import EngineManager
from sparql_conformance import util
from sparql_conformance.models import Config
from qlever.commands.index import IndexCommand
from sparql_conformance.rdf_tools import write_ttl_file, delete_ttl_file, rdf_xml_to_turtle


class QLeverManager(EngineManager):
    """Manager for QLever using docker execution"""

    def cleanup(self, config: Config):
        self._stop_server(config.port)
        with mute_log():
            run_command('rm -f sparql-conformance-index*')

    def query(self, config: Config, query: str, query_type: str, result_format: str) -> Tuple[int, str]:
        content_type = "query=" if query_type == "rq" else "update="
        args = Namespace(
            query=query,
            host_name=config.server_address,
            port=config.port,
            sparql_endpoint=None,
            accept=self._get_accept_header(result_format),
            access_token='abc',
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
        server_success = False
        graphs = []
        for graph_path, graph_name in graph_paths:
            # Handle rdf files by turning them into turtle format.
            if graph_path.endswith(".rdf"):
                graph_path_new = Path(graph_path).name
                graph_path_new = graph_path_new.replace(".rdf", ".ttl")
                write_ttl_file(graph_path_new, rdf_xml_to_turtle(graph_path, graph_name))
                graph_path = graph_path_new
            else:
                graph_path = self._copy_graph_to_workdir(graph_path, os.getcwd())
            graphs.append((graph_path, graph_name))

        index_success, index_log = self._index(graphs)
        if not index_success:
            return index_success, server_success, index_log, ''
        else:
            server_success, server_log = self._start_server(
                config.server_address,
                config.port)

            if not server_success:
                return index_success, server_success, index_log, server_log
        for path, name in graphs:
            delete_ttl_file(path)
        return index_success, server_success, index_log, server_log

    def _stop_server(self, port: int) -> Tuple[bool, str]:
        args = Namespace(
            name='sparql-conformance-index',
            port=port,
            server_container='sparql-conformance-server',
            cmdline_regex=f"^ServerMain.* -p {port}",
            no_containers=False,
            show=False
        )
        try:
            with mute_log():
                result = StopCommand().execute(args)
        except Exception as e:
            error_output = str(e)
            return False, error_output
        return result, 'Success'

    def _start_server(self, host: str, port: int) -> Tuple[bool, str]:
        args = Namespace(
            name='sparql-conformance-index',
            server_binary='ServerMain',
            host_name=host,
            port=port,
            access_token='abc',
            memory_for_queries='4GB',
            cache_max_size='1GB',
            cache_max_size_single_entry='100MB',
            cache_max_num_entries=1000000,
            num_threads=1,
            timeout=None,
            persist_updates=False,
            only_pso_and_pos_permutations=False,
            use_patterns=True,
            use_text_index='no',
            warmup_cmd=None,
            system='docker',
            image='docker.io/adfreiburg/qlever:latest',
            server_container='sparql-conformance-server',
            description=None,
            text_description=None,
            kill_existing_with_same_port=False,
            no_warmup=True,
            run_in_foreground=False,
            show=False
        )
        try:
            with mute_log():
                result = StartCommand().execute(args, called_from_conformance_test=True)
        except Exception as e:
            error_output = str(e)
            return False, error_output

        server_log = ''
        if os.path.exists('./sparql-conformance-index.server-log.txt'):
            server_log = util.read_file('./sparql-conformance-index.server-log.txt')
        return result, server_log

    def _index(self, graph_paths: List[Tuple[str, str]]) -> Tuple[bool, str]:
        args = Namespace(
            name='sparql-conformance-index',
            cat_input_files=None,
            multi_input_json=self._generate_multi_input_json(graph_paths),
            input_files='*.ttl',
            format='ttl',
            settings_json='{ "num-triples-per-batch": 1000000 }',
            system='docker',
            image='docker.io/adfreiburg/qlever:latest',
            parallel_parsing=False,
            only_pso_and_pos_permutations=False,
            use_patterns=True,
            text_index=None,
            stxxl_memory=None,
            parser_buffer_size=None,
            ulimit=None,
            show=None,
            overwrite_existing=True,
            index_binary='IndexBuilderMain',
            index_container='sparql-conformance-container'
        )
        try:
            with mute_log():
                result = IndexCommand().execute(args=args, called_from_conformance_test=True)
        except Exception as e:
            error_output = str(e)
            return False, error_output

        index_log = ''
        if os.path.exists("./sparql-conformance-index.index-log.txt"):
            index_log = util.read_file("./sparql-conformance-index.index-log.txt")
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

    def _copy_graph_to_workdir(self, graph_path: str, workdir: str) -> str:
        """
        Copy the graph to the docker working directory and returns the new relative path.

        Args:
            graph_path (str): Path to the source file.
            workdir (str): Path to the working directory mounted in docker.

        Returns:
            str: Basename, usable inside the container.
        """
        src = Path(graph_path).resolve()
        dest = Path(workdir).resolve() / src.name
        shutil.copy(src, dest)
        return src.name

    def _get_accept_header(self, result_format: str) -> str:
        format_headers = {
            "csv": "text/csv",
            "tsv": "text/tab-separated-values",
            "srx": "application/sparql-results+xml",
            "ttl": "text/turtle",
            "json": "application/sparql-results+json"
        }
        return format_headers.get(result_format, "application/sparql-results+json")