from .engines.manager import EngineManager
from .engines.qlever_binary import QLeverBinaryManager

def get_engine_manager(engine_type: str) -> EngineManager:
    """Get the appropriate engine manager for the given engine type"""
    managers = {
        'qlever-binaries': QLeverBinaryManager,
        'qlever': None,  # To be implemented
        'qmdb': None,  # To be implemented
    }

    manager_class = managers.get(engine_type)
    if manager_class is None:
        raise ValueError(f"Unsupported engine type: {engine_type}")

    return manager_class()
