import pytest

from utils.query_builder import build_fact_query
from utils.transformation import reusable


class FakeDF:
    def __init__(self):
        self.dropped = None

    def drop(self, *cols):
        self.dropped = cols
        return self


def test_drop_columns_passes_all_names():
    df = reusable().dropColumns(FakeDF(), ["_rescued_data", "x"])
    assert df.dropped == ("_rescued_data", "x")


DIM = {"table": "c.gold.DimUser", "alias": "u", "fact_key": "user_id", "dim_key": "user_id", "columns": ["country"]}


def test_query_joins_current_dimension_rows():
    q = build_fact_query("c.silver.FactStream", [DIM])
    assert "FROM c.silver.FactStream f" in q
    assert "LEFT JOIN c.gold.DimUser u" in q
    assert "ON f.user_id = u.user_id" in q
    assert "u.__END_AT IS NULL" in q
    assert "u.country AS u_country" in q


def test_query_with_multiple_dims():
    other = {**DIM, "table": "c.gold.DimTrack", "alias": "t", "fact_key": "track_id", "dim_key": "track_id"}
    q = build_fact_query("f_tbl", [DIM, other])
    assert q.count("LEFT JOIN") == 2


def test_missing_config_key_raises():
    with pytest.raises(ValueError):
        build_fact_query("f", [{"table": "x", "alias": "a"}])
