class FeaflowException(Exception):
    pass


class ConfigLoadError(FeaflowException):
    def __init__(self, config_path: str):
        super().__init__(f"Could not load config file '{config_path}`")


class ClassImportError(Exception):
    def __init__(self, class_name: str, extra_info: str = ""):
        super().__init__(f"Could not import class '{class_name}', {extra_info}")


class EngineInitError(Exception):
    def __init__(self, engine_type: str):
        super().__init__(f"Could not initialize engine '{engine_type}'")


class EngineRunError(Exception):
    def __init__(self, engine_type: str, msg: str):
        super().__init__(f"Engine '{engine_type}' running failed with msg {msg}")
