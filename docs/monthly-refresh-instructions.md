# Monthly refresh helper (Synapse/Fabric)

This PR introduces a small helper that lets you rebuild only the current month window every day. It uses a bounded DELETE+rebuild pattern and falls back to your original incremental append behavior.

## 1) Import the helper in your Synapse/Fabric notebook
After your existing imports, add:

```python
from utils.monthly_refresh import update_table_helper, first_day_of_month
```

## 2) Use refresh_current_month=True on your daily updates
Replace your three update calls with the versions below.

Share events table (EventDate):
```python
update_table_helper(
    max_valid_date_in_pageview,
    get_shares_table,
    "verifiable_credentials_report_share_events_table",
    refresh_current_month=True
)
```

VC base table (AwardedOn) and cascading faux entries:
```python
update_table_helper(
    max_valid_date_in_pageview,
    get_vc_pure, 
    "verifiable_credentials_inner_table_vc",
    column_name="AwardedOn",
    colmn_name_in_condition="to_date(AwardedOn)",
    result_funcion=append_the_new_rows_to_unagg_all_share,
    refresh_current_month=True
)
```

Skill share aggregate (EventDate) and cascading unagg recompute:
```python
update_table_helper(
    max_valid_date_in_pageview,
    get_skill_shares_agg, 
    "verifiable_credentials_inner_table_skill_share_aggregate",
    result_funcion=alter_the_unagg_all_share_for_the_new_skill_share_agg,
    refresh_current_month=True
)
```

## 3) Optional: align lang/loc backfill for current month
If you want `verifiable_credential_inner_table_SiteUserIdHash_to_lang_loc` to reflect only the current month window each day:

```python
start_of_month = first_day_of_month(max_valid_date_in_pageview)
som_str = start_of_month.strftime("%Y-%m-%d")
print("Updating lang/loc table for current month window")

spark.sql(f"""
    DELETE FROM verifiable_credential_inner_table_SiteUserIdHash_to_lang_loc
    WHERE EventDate >= DATE'{som_str}'
""")

get_webTelemetryAppliedSkills(
    f" pvl.EventDate >= '{som_str}' "
).write.mode("append").saveAsTable(
    "verifiable_credential_inner_table_SiteUserIdHash_to_lang_loc"
)

spark.table(
    "verifiable_credential_inner_table_SiteUserIdHash_to_lang_loc"
).groupBy(
    "SiteUserIdHash"
).agg(
    spark_func.mode(spark_func.col("webLanguage")).alias("webLanguage"),
    spark_func.mode(spark_func.col("UserCountry")).alias("UserCountry")
).write.mode(
    "overwrite"
).saveAsTable(
    "verifiable_credential_inner_table_SiteUserIdHash_to_lang_loc"
)
```

## Notes
- This approach is idempotent for the current month. You can run it multiple times per day.
- For very large tables, consider partitioning on EventDate/AwardedOn and using `replaceWhere` for faster month refreshes.