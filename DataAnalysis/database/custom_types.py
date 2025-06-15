from .. import Literal, pd, TypeAlias, TypedDict







TableData:TypeAlias = dict[str, pd.DataFrame]
TableMap:TypeAlias = dict[Literal['table_order_map'], TableData]
DatabaseConfigMap:TypeAlias = dict[str, TableMap]


class ProccessingConfig(TypedDict):
    data_aliases: dict[Literal['purchase_data', 'flavor_data', 'can_data'], str]
    default_output_directory_names: dict[str, str]
    headers_to_extract: dict[Literal['flavor', 'can'], list[str]]

TablesMicroInfo:TypeAlias = dict[Literal['header'], list[str]]
TablesMacroInfo:TypeAlias = dict[str, TablesMicroInfo] # NOTE key is table name    
