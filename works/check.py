import polars as pl

def check_df(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, float, int]:
    correct_df = df.with_columns(
        (
            df["Стоимость продаж с НДС в руб. и коп. (стр. 160)"]
            - (
                df["Сумма НДС"]
                + df[
                    "Стоимость продаж облагаемых налогом всего (без суммы НДС, стр. 170 + 175 + 180 + 190)"
                ]
            )
        ).alias("Разница сумм"),
    ).with_columns((pl.col("Разница сумм") <= 3).alias("корректная обработка"))
    summary_parts: float = (
        correct_df.with_columns(
            correct_df["Доля продаж (стр. 160 + 210)"]
            .str.replace(",", ".")
            .str.head(-1)
            .cast(pl.Float64, strict=False)
        )["Доля продаж (стр. 160 + 210)"]
        .sum()
        .__round__(2)
    )

    problems_df = correct_df.filter(correct_df["корректная обработка"].not_())
    h = problems_df.height
    print(f"Сумма процентов доли продаж = {summary_parts}")
    return (correct_df, problems_df, summary_parts, h)
