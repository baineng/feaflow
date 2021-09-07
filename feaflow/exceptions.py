class FeaflowException(Exception):
    def __init__(self, *args, **kwargs):
        pass


class NotAllowedException(FeaflowException):
    def __init__(self, *args, **kwargs):
        pass


class ConfigException(FeaflowException):
    def __init__(self, err_msg, config_path):
        self._err_msg = err_msg
        self._config_path = config_path
        super().__init__(self._err_msg)

    def __str__(self) -> str:
        return f"{self._err_msg}\nat {self._config_path}"

    def __repr__(self) -> str:
        return f"ConfigException({repr(self._err_msg)}, {repr(self._config_path)})"
