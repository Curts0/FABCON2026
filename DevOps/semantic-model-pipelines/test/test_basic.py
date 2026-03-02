import pytabular as p
import pytest

def test_sanity():
    1 == 1

def test_model(model):
    assert isinstance(model, p.Tabular)

def test_tables(table):
    assert table.row_count() > 0, f"Uh oh {table.Name} has 0 rows."

def test_columns(column):
    assert column.distinct_count() > 0, f"Uh oh {column.Table.Name}[{column.Name}] is blank."