class FeaflowException(Exception):
    pass


class JobNotFoundError(FeaflowException):
    def __init__(self, project_root: str, job_name: str):
        super().__init__(
            f"Could not find job '{job_name}' in project directory '{project_root}'"
        )


class ConfigLoadError(FeaflowException):
    def __init__(self, config_path: str):
        super().__init__(f"Could not load config file '{config_path}`")


class ClassImportError(Exception):
    def __init__(self, class_name: str, extra_info: str = ""):
        super().__init__(f"Could not import class '{class_name}', {extra_info}")


class EngineInitError(Exception):
    def __init__(self, engine_type: str):
        super().__init__(f"Could not initialize engine '{engine_type}'")


class EngineHandleError(Exception):
    def __init__(self, reason, context, unit):
        super().__init__(
            f"Engine handles unit '{unit}' failed, because of: '{reason}', with context: '{context}'"
        )
