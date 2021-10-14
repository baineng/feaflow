class FeaflowException(Exception):
    pass


class JobNotFoundError(FeaflowException):
    def __init__(self, project_root: str, job_name: str):
        super().__init__(
            f"Could not find job '{job_name}' in project directory '{project_root}'"
        )


class ConfigLoadError(FeaflowException):
    def __init__(self, config_path: str, reason: str = ""):
        super().__init__(f"Could not load config file '{config_path}', {reason}")


class ClassImportError(Exception):
    def __init__(self, class_name: str, extra_info: str = ""):
        super().__init__(f"Could not import class '{class_name}', {extra_info}")


class EngineInitError(Exception):
    def __init__(self, engine_type: str):
        super().__init__(f"Could not initialize engine '{engine_type}'")


class EngineHandleError(Exception):
    def __init__(self, reason, comp):
        super().__init__(
            f"Engine handles component '{comp}' failed, reason: '{reason}'"
        )


class EngineExecuteError(Exception):
    def __init__(self, reason, exec_env, exec_dag):
        super().__init__(
            f"Engine executes task failed, reason: '{reason}', environment: '{exec_env}'"
        )
