import polars as pl


def get_empty_dim_table(col: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            f"{col}_id": [],
            col: [],
        },
        schema={f"{col}_id": pl.Int64, col: pl.Utf8},
    )


def update_dim_table(existing_df: pl.DataFrame, new_df: pl.DataFrame, col: str) -> pl.DataFrame:
    new_entries = new_df.select(col).unique().join(existing_df, on=col, how="anti")

    # get None from max() if the table is empty
    max_id = existing_df[f"{col}_id"].max() or 0

    new_entries = new_entries.with_row_index(name=f"{col}_id", offset=max_id + 1).with_columns(
        pl.col(f"{col}_id").cast(pl.Int64),
    )
    return pl.concat([existing_df, new_entries])
