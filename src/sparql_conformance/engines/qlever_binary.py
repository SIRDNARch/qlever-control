import time
from typing import Tuple

import requests
import subprocess
from sparql_conformance.engines.manager import EngineManager
from sparql_conformance.rdf_tools import write_ttl_file, delete_ttl_file, rdf_xml_to_turtle


class QLeverBinaryManager(EngineManager):
    """Manager for QLever using binary execution"""

    def index(self, command_index: str, graph_paths: list) -> Tuple[bool, str]:
        print("TEST")
        remove_paths = []
        graphs = ""
        for graph in graph_paths:
            graph_path = graph[0]
            graph_name = graph[1]
            if graph_path.endswith(".rdf"):
                graph_path_new = graph_path.replace(".rdf", ".ttl")
                remove_paths.append(graph_path_new)
                write_ttl_file(graph_path_new, rdf_xml_to_turtle(graph_path))
                graph_path = graph_path_new
            graphs += f" -f {graph_path} -F ttl -g {graph_name}"

        status = False
        try:
            cmd = command_index + graphs
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            output, error = process.communicate()
            if process.returncode != 0:
                print(error.decode('utf-8'))
                return status, f"Indexing error: {error.decode('utf-8')} \n \n {output.decode('utf-8')}"
            index_log = output.decode("utf-8")
            if "Index build completed" in index_log:
                status = True
            for path in remove_paths:
                delete_ttl_file(path)
            print(index_log)
            return status, index_log
        except Exception as e:
            return status, f"Exception executing index command: {str(e)}"

    def remove_index(self, command_remove_index: str) -> Tuple[bool, str]:
        try:
            subprocess.check_call(command_remove_index, shell=True)
            return True, ""
        except subprocess.CalledProcessError as e:
            return False, f"Error removing index files: {e}"

    def start_server(self, command_start_server: str, server_address: str, port: str) -> Tuple[int, str]:
        try:
            subprocess.Popen(command_start_server, shell=True)
            return self._wait_for_server_startup(server_address, port)
        except Exception as e:
            return (500, f"Exception executing server command: {str(e)}")

    def stop_server(self, command_stop_server: str) -> str:
        try:
            subprocess.check_call(command_stop_server, shell=True)
            return ""
        except subprocess.CalledProcessError as e:
            return f"Error stopping server: {e}"

    def query(self, query: str, query_type: str, result_format: str,
              server_address: str, port: str) -> Tuple[int, str]:
        accept = self._get_accept_header(result_format)
        content_type = "application/sparql-query; charset=utf-8" if query_type == "rq" else "application/sparql-update; charset=utf-8"

        url = f"{server_address}:{port}?access-token=abc"
        headers = {"Accept": accept, "Content-type": content_type}
        try:
            response = requests.post(url, headers=headers, data=query.encode("utf-8"))
            return (response.status_code, response.content.decode("utf-8"))
        except requests.exceptions.RequestException as e:
            return (500, f"Query execution error: {str(e)}")

    def _wait_for_server_startup(self, server_address: str, port: str) -> Tuple[int, str]:
        max_retries = 8
        retry_interval = 0.25
        url = f"{server_address}:{port}"
        headers = {"Content-type": "application/sparql-query"}
        test_query = "SELECT ?s ?p ?o { ?s ?p ?o } LIMIT 1"

        for i in range(max_retries):
            try:
                response = requests.post(url, headers=headers, data=test_query)
                if response.status_code == 200:
                    return (200, "Server ready!")
            except requests.exceptions.RequestException:
                pass
            time.sleep(retry_interval)

        return (500, "Server failed to start within expected time")

    def _get_accept_header(self, result_format: str) -> str:
        format_headers = {
            "csv": "text/csv",
            "tsv": "text/tab-separated-values",
            "srx": "application/sparql-results+xml",
            "ttl": "text/turtle",
            "json": "application/sparql-results+json"
        }
        return format_headers.get(result_format, "application/sparql-results+json")
