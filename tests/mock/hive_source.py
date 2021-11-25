from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto


class HiveSource(DataSource):
    def __init__(
        self,
        table: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self.table = table

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        pass

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
        )
        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        def s_to_f(s: str) -> ValueType:
            return ValueType.INT32

        return s_to_f

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        return [
            ("id", "int"),
            ("send_amount", "int"),
            ("login_times", "int"),
            ("logout_times", "int"),
        ]

    def get_table_query_string(self) -> str:
        return f"`{self.table}`"
