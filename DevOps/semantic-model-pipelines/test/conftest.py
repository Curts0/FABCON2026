import pytest
import pytabular as p
import pathlib


MODEL = None

def pytest_addoption(parser):
    parser.addoption(
        "--model", action="store"
    )

@pytest.fixture(scope="session", autouse=True)
def model(request):
    global MODEL
    return MODEL

def pytest_generate_tests(metafunc):
    global MODEL
    if "table" in metafunc.fixturenames:
        metafunc.parametrize(
            "table",
            MODEL.Tables,
            ids = [table.Name for table in MODEL.Tables]
        )
    if "column" in metafunc.fixturenames:
         all_columns = []
         all_ids = []
         for table in MODEL.Tables:
            for column in table.Columns:
                all_columns.append(column)
                all_ids.append(f"{table.Name}[{column.Name}]")
         metafunc.parametrize(
            "column",
            all_columns,
            ids = all_ids
        )
    if "measure" in metafunc.fixturenames:
         metafunc.parametrize(
            "measure",
            MODEL.Measures,
            ids = [measure.Name for measure in MODEL.Measures]
        )    
    if "query" in metafunc.fixturenames:
         all_files = list(pathlib.Path("./Dax Queries").rglob("*.dax"))
         metafunc.parametrize(
            "query",
            all_files,
            ids=[file.name for file in all_files]
        )    

def pytest_sessionstart(session):
    global MODEL
    MODEL = p.Tabular(session.config.getoption("model"))