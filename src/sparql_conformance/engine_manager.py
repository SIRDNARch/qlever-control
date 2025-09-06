from .engines.manager import EngineManager
from .engines.oxigraph import OxigraphManager
from .engines.qlever_binary import QLeverBinaryManager
from .engines.qlever import QLeverManager
from .engines.mdb import MDBManager


def get_engine_manager(engine_type: str) -> EngineManager:
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
