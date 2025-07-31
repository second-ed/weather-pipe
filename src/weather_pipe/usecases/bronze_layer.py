import attrs
import polars as pl
from returns.pipeline import is_successful

import weather_pipe.domain.data_structures as ds
import weather_pipe.domain.transform as tf
from weather_pipe.service_layer.uow import UnitOfWorkProtocol
from weather_pipe.usecases._event import Event


@attrs.define
class PromoteToBronzeLayer(Event):
    db_path: str = attrs.field(default="")


def bronze_layer_handler(event: PromoteToBronzeLayer, uow: UnitOfWorkProtocol) -> None:
    paths = uow.repo.list(event.src_root)

    failed_files = {}

    for path in paths:
        big_table = pl.read_parquet(path)
        res = tf.unnest_struct_cols(ds.RawTable(table=big_table)).bind(tf.clean_text_cols)
        if not is_successful(res):
            failed_files[path] = res
            continue

        # update dim tables
        # update fact table
        # delete (or archive) file
