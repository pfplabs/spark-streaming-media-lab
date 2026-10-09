# Spark for Streaming Media Analytics

Runnable companion to the eight PFP Labs draft chapters. All viewers, titles, and events are synthetic. The blog chapters remain unpublished and require admin access. Each row links to the intended public chapter URL and a private review page; the public URL will not display the draft before release.

## Local setup

Use Python 3.11 or 3.12 and Java 17. Set JAVA_HOME to your JDK if needed. Run from the repository root:

```sh
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export SPARK_LOCAL_IP=127.0.0.1
pytest -q
python chapters/01_mental_model.py
python chapters/02_ingestion.py
python chapters/03_sessions.py
python chapters/04_performance.py
python chapters/05_streaming.py --seconds 45
python chapters/06_quality.py
python chapters/07_genie_code.py
python chapters/08_capstone.py
python chapters/08_capstone.py
```

First use downloads Delta JVM artifacts from Maven (internet required). Spark 3.5.6 and Delta 3.3.2 are a compatible pair. Data and checkpoints live in `.lab/`; override with MEDIA_LAB_ROOT. Use N_EVENTS=2000 for a quick smoke run. Chapters 1, 3, 4, 6, and 7 generate their own batch data; chapter 8 requires chapter 2. Chapter 2 intentionally appends bronze on each run. Silver deduplicates the complete synthetic event payload before sessionization and uses an upsert, so repeating the same input preserves silver/gold counts. This is a full-history teaching pipeline, not an incremental late-arrival sessionization design. No production credentials or real data are required.

## Chapter map

| Part | Example | PFP Labs draft |
| --- | --- | --- |
| 1 | [01_mental_model](chapters/01_mental_model.py) | [Chapter 1](https://pfplabs.ai/blog/pyspark-mental-model-streaming-media-analytics) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/pyspark-mental-model-streaming-media-analytics) |
| 2 | [02_ingestion](chapters/02_ingestion.py) | [Chapter 2](https://pfplabs.ai/blog/pyspark-schemas-ingestion-delta-bronze) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/pyspark-schemas-ingestion-delta-bronze) |
| 3 | [03_sessions](chapters/03_sessions.py) | [Chapter 3](https://pfplabs.ai/blog/pyspark-window-functions-viewer-sessions) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/pyspark-window-functions-viewer-sessions) |
| 4 | [04_performance](chapters/04_performance.py) | [Chapter 4](https://pfplabs.ai/blog/spark-performance-shuffles-partitions-broadcast) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/spark-performance-shuffles-partitions-broadcast) |
| 5 | [05_streaming](chapters/05_streaming.py) | [Chapter 5](https://pfplabs.ai/blog/spark-structured-streaming-watermarks-concurrent-viewers) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/spark-structured-streaming-watermarks-concurrent-viewers) |
| 6 | [06_quality](chapters/06_quality.py) | [Chapter 6](https://pfplabs.ai/blog/pyspark-data-quality-testing-pipelines) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/pyspark-data-quality-testing-pipelines) |
| 7 | [07_genie_code](chapters/07_genie_code.py) | [Chapter 7](https://pfplabs.ai/blog/databricks-genie-code-pyspark-review-workflow) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/databricks-genie-code-pyspark-review-workflow) |
| 8 | [08_capstone](chapters/08_capstone.py) | [Chapter 8](https://pfplabs.ai/blog/spark-medallion-pipeline-streaming-media-capstone) · [Admin review](https://id-preview--8a137d1f-c1b0-4dc9-a0b4-a735ec35e1b4.lovable.app/admin/drafts/spark-medallion-pipeline-streaming-media-capstone) |

## Contracts and exercises

- Part 1: inspect laziness; count event types and rank TV completions. The original generator puts each viewer's successive events about 9.7 hours apart, so most sample sessions contain one event. Use handcrafted fixtures to learn session boundaries. watch_seconds is a per-event contribution, not cumulative player position.
- Part 2: explicit schema, two malformed rows, file lineage, append-only Delta bronze. JSON nullable=False is not a required-field validator; apply quality checks. Try FAILFAST and Delta history.
- Part 3: 30-minute gap (strict >), session-title completion rate, broadcast catalog and dense ranking. Equal rankings can return more than three titles. Try a 10-minute gap and SQL equivalent.
- Part 4: seeded skew and two-stage salted sums. Compare with the unsalted sum and inspect Exchange nodes; do not infer real cluster speedups from this tiny local dataset.
- Part 5: approximate distinct heartbeat viewers per title per minute, an activity proxy rather than exact simultaneous concurrency. Append emits finalized windows only after the watermark advances. A 45-second smoke run may emit no finalized rows; run at least 240 seconds to observe output. Try a shorter watermark and inspect numRowsDroppedByWatermark. Keep checkpoint and output together; deleting only a checkpoint can duplicate output. Stop rows are not used to track active sessions.
- Part 6: null-safe quality flags and pytest fixtures. Add future-time checks and per-check failures.
- Part 7: [Databricks workspace prompts and review procedure](chapters/07_genie_code.md). Local fixture runs without AI access; workspace execution is manual.
- Part 8: rerunnable silver upsert and full-rebuild daily gold. Run twice and compare counts. Schedule streaming separately; add daily device mix and write its definition first.

## Runbook

For unexpected gold results: check bronze input/quarantine counts, quality rejections, duplicate payloads, session gap/tie policy, catalog coverage and UTC day boundaries. completion_event_share in gold is completed events / all valid events; it differs from part 3's session-title completion rate. Check streaming progress/state and watermark before assuming missing output. Never reset a checkpoint against an existing output as a normal restart.

## References

[PySpark 3.5.6](https://spark.apache.org/docs/3.5.6/api/python/index.html), [Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html), [Delta compatibility](https://docs.delta.io/releases/), [Genie Code](https://docs.databricks.com/aws/en/genie-code/).
