from typing import Callable, Any
from datetime import datetime, date, timedelta

# NOTE: This helper assumes you are running inside a Spark environment where `spark` is available.
# It performs a bounded DELETE on a Delta table and then recomputes the same window.


def first_day_of_month(d: date) -> date:
    return d.replace(day=1)


def update_table_helper(
    max_valid_date_in_pageview: date,
    update_function: Callable[[str], Any],
    table_name: str,
    column_name: str = "EventDate",
    colmn_name_in_condition: str = "EventDate",
    result_funcion: Callable[[Any], Any] = (lambda x: None),
    refresh_current_month: bool = False,
):
    """
    When refresh_current_month=True:
      - Determine the month window [first_day_of_month(max_valid_date_in_pageview), max_valid_date_in_pageview]
      - DELETE that window in target table
      - Recompute and append rows just for that window
    Else:
      - Original behavior: append (max_date_in_table, max_valid_date_in_pageview]

    Notes:
      - Expects Delta tables (DELETE support).
      - colmn_name_in_condition allows expressions like to_date(AwardedOn).
    """

    def fmt(d: date) -> str:
        return d.strftime("%Y-%m-%d")

    res = None

    if refresh_current_month:
        start_date = first_day_of_month(max_valid_date_in_pageview)
        start_str = fmt(start_date)
        end_str = fmt(max_valid_date_in_pageview)

        print(f"Refreshing current month window for {table_name}: {start_str}..{end_str}")

        delete_sql = f"""
            DELETE FROM {table_name}
            WHERE DATE'{start_str}' <= {column_name} AND {column_name} <= DATE'{end_str}'
        """
        print(delete_sql)
        spark.sql(delete_sql)

        update_condition = (
            f"DATE'{start_str}' <= {colmn_name_in_condition} AND "
            f"{colmn_name_in_condition} <= DATE'{end_str}'"
        )
        print(f"Recomputing with condition: {update_condition}")
        df = update_function(update_condition)
        res = result_funcion(df)
        df.write.mode("append").format("delta").saveAsTable(table_name)
        return res

    # Fallback: original incremental append
    FALLBACK_DATE = datetime.strptime("2020-01-01", "%Y-%m-%d").date()

    def get_max_date_in_table(table_name_: str, column_name_: str = "EventDate"):
        return spark.sql(f"select max({column_name_}) as max_date from {table_name_}").collect()[0].max_date

    max_date_in_update_table = get_max_date_in_table(table_name, column_name)
    if max_date_in_update_table is None:
        max_date_in_update_table = FALLBACK_DATE

    max_date_in_update_table_strf = fmt(max_date_in_update_table)
    max_valid_date_in_pageview_strf = fmt(max_valid_date_in_pageview)

    print(f"max date in {table_name}::\t{max_date_in_update_table}")
    if max_date_in_update_table < max_valid_date_in_pageview:
        update_condition = (
            f"DATE'{max_date_in_update_table_strf}' < {colmn_name_in_condition} and "
            f"{colmn_name_in_condition} <= Date'{max_valid_date_in_pageview_strf}'"
        )
        print(f"{table_name} is getting updated cause of date difference")
        print(update_condition)
        df = update_function(update_condition)
        res = result_funcion(df)
        df.write.mode("append").format("delta").saveAsTable(table_name)
    return res