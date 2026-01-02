import pyarrow as pa
from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate non_plastic_substitutes_trade dataset."""
    raise NotImplementedError("Test not yet implemented - implement after transform")

    # validate(table, {
    #     "columns": {
    #         # TODO: Add columns after profiling
    #     },
    #     "not_null": [],
    #     "min_rows": 100,
    # })
