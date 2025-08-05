import tempfile

import polars as pl

from tests.conftest import RAW_LAYER_FACT
from weather_pipe.adapters import repo
from weather_pipe.adapters.fs_wrappers._fs_protocol import FakeFileSystem
from weather_pipe.adapters.io_wrappers._io_protocol import FakeIOWrapper
from weather_pipe.adapters.logger import FakeLogger
from weather_pipe.service_layer.uow import UnitOfWork
from weather_pipe.usecases.bronze_layer import PromoteToBronzeLayer, bronze_layer_handler


def test_bronze_layer():
    db = {
        "some/path/root/file1.parquet": pl.DataFrame(RAW_LAYER_FACT),
    }

    with tempfile.NamedTemporaryFile(suffix=".db", delete_on_close=False) as tmp:
        bronze_repo = repo.Repo(
            io=FakeIOWrapper(db=db, db_name=tmp.name),
            fs=FakeFileSystem(db=db),
        )
        uow = UnitOfWork(bronze_repo, FakeLogger())
        event = PromoteToBronzeLayer(
            src_root="some/path/root",
            table_names=["wind_dir", "text", "icon"],
        )
        res = bronze_layer_handler(event, uow)
        assert res.is_ok()

        # run again to cover when the dim tables already exist
        res = bronze_layer_handler(event, uow)
        assert res.is_ok()
