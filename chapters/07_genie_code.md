# Genie Code: review-first workspace exercise

Use a Databricks Git folder to clone this repository. Use a supported Databricks Runtime; do not install local Spark pins into managed compute. Set a writable Unity Catalog Volume path instead of `.lab`. Run the generator and ingestion logic in a notebook using its existing `spark` session; Databricks supplies Delta. Register your synthetic bronze table in your own catalog/schema before referring to it with @.

Prompt: Using @bronze_playback, assign session_n per viewer_id after **more than** 1800 seconds of inactivity. Order by event_ts, title_id, event_type, watch_seconds. Use windows, no UDF. Explain identical duplicate handling. Do not modify or write tables.

Review the proposed code against `transforms.py` and `tests/test_transforms.py`. Run the small fixture in `07_genie_code.py` with the notebook session. Check exact gaps, ties, nulls, plan shape, and authorized table access. Record prompt, proposed diff, test results, and rejected suggestions in your own review log. No generated results are supplied or claimed here.

Exercise: ask for SQL equivalent, compare plans, then ask for one optimization without changing metric definitions. Accept only after matching tests and explaining the result.

[Current official Genie Code documentation](https://docs.databricks.com/aws/en/genie-code/). Availability and costs depend on workspace/account settings.
