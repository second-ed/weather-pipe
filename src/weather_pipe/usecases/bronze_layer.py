import attrs

import weather_pipe.domain.data_structures as ds
import weather_pipe.domain.transform as tf
from weather_pipe.adapters.io_wrappers._io_protocol import FileType
from weather_pipe.domain import utils
from weather_pipe.domain.result import Err
from weather_pipe.service_layer.uow import UnitOfWorkProtocol
from weather_pipe.usecases._event import Event


@attrs.define
class PromoteToBronzeLayer(Event):
    src_root: str = attrs.field(default="")
    table_names: list = attrs.field(default=attrs.Factory(list))


def bronze_layer_handler(event: PromoteToBronzeLayer, uow: UnitOfWorkProtocol) -> None:
    with uow:
        paths = uow.repo.fs.list(event.src_root)
        uow.logger.info({"guid": uow.guid, "msg": f"found {len(paths)} files"})

        failed_reads, failed_writes = [], []

        for path in paths:
            raw_df = uow.repo.io.read(path, FileType.PARQUET)

            if not raw_df.is_ok():
                failed_reads.append(raw_df)
                uow.logger.error(
                    {"guid": uow.guid, "msg": f"failed to read fact table for path: `{path}`"},
                )
                continue

            raw_df = raw_df.unwrap()
            cleaned_fact = tf.unnest_struct_cols(ds.RawTable(table=raw_df)).bind(tf.clean_text_cols)

            if not cleaned_fact.is_ok():
                failed_reads.append(cleaned_fact)
                uow.logger.error(
                    {
                        "guid": uow.guid,
                        "msg": f"failed to unnest and clean fact table for path: `{path}`",
                    },
                )
                continue

            cleaned_fact = cleaned_fact.unwrap()

            dim_tables = {}

            for table_name in event.table_names:
                update_dim_table(uow, cleaned_fact, table_name, failed_reads, failed_writes)
                dim_data = uow.repo.io.read(f"SELECT * FROM {table_name}", FileType.SQLITE3)  # noqa: S608

                if not dim_data.is_ok():
                    failed_reads.append(dim_data)
                    uow.logger.error(
                        {"guid": uow.guid, "msg": f"failed to read table: `{table_name}`"},
                    )
                    continue

                dim_tables[table_name] = dim_data.unwrap()

            encoded_fact = tf.replace_col_with_id(cleaned_fact, dim_tables)

            if not encoded_fact.is_ok():
                uow.logger.error(
                    {
                        "guid": uow.guid,
                        "msg": f"failed to replace cols with ids for table: `{encoded_fact}`",
                    },
                )
                continue

            encoded_fact = encoded_fact.unwrap()

            res = uow.repo.io.write(
                encoded_fact.table,
                "fact_weather",
                FileType.SQLITE3,
                if_table_exists="append",
            )

            if not res.is_ok():
                failed_writes.append(res)
                uow.logger.error(
                    {"guid": uow.guid, "msg": f"failed write fact_table for path: `{path}`"},
                )
                continue

            uow.logger.info(
                {"guid": uow.guid, "msg": f"successfully updated fact table for path: `{path}`"},
            )


def update_dim_table(
    uow: UnitOfWorkProtocol,
    cleaned_fact: ds.CleanedTable,
    table_name: str,
    failed_reads: list[Err],
    failed_writes: list[Err],
) -> bool:
    table_exists = uow.repo.io.conn.execute(
        f"SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name='{table_name}'",  # noqa: S608
    ).fetchall()

    if table_exists:
        uow.logger.info(
            {"guid": uow.guid, "msg": f"table exists, attempting to read table `{table_name}`"},
        )
        dim_table = uow.repo.io.read(f"SELECT * FROM {table_name}", FileType.SQLITE3)  # noqa: S608

        if not dim_table.is_ok():
            failed_reads.append(dim_table)
            uow.logger.error({"guid": uow.guid, "msg": f"failed to read table `{table_name}`"})
            return False

        uow.logger.info({"guid": uow.guid, "msg": f"successfully read table `{table_name}`"})
        dim_table = dim_table.unwrap()
    else:
        uow.logger.info(
            {"guid": uow.guid, "msg": f"`{table_name}` doesn't exist, creating empty one"},
        )
        dim_table = utils.get_empty_dim_table(table_name)

    updated_dim_table = utils.update_dim_table(
        dim_table,
        cleaned_fact.table,
        table_name,
    )
    if updated_dim_table.equals(dim_table):
        uow.logger.info(
            {"guid": uow.guid, "msg": f"no update to dim table `{table_name}`, skipping write"},
        )
        return False

    write_result = uow.repo.io.write(
        dim_table,
        table_name,
        FileType.SQLITE3,
        if_table_exists="replace",
    )

    if not write_result.is_ok():
        failed_writes.append(write_result)
        uow.logger.error({"guid": uow.guid, "msg": f"failed to write table `{table_name}`"})
        return False

    uow.logger.info({"guid": uow.guid, "msg": f"successfully updated table `{table_name}`"})
    return True
