# Technical Documentation
## Serie A Colombian Players Analytics Pipeline
### Engineering Reference

This document is the complete engineering reference for the Serie A 
Colombian Players Analytics Pipeline — covering architectural decisions, 
technology selection rationale, implementation details, and design 
philosophy across all pipeline layers.

For a high-level project overview, start with the README.  
This document is intended for technical reviewers.

---

## Table of Contents

1. [Pipeline Architecture Overview](#1-pipeline-architecture-overview)
2. [Data Extraction Layer — Bronze](#2-data-extraction-layer--bronze)
3. [Data Transformation Layer — Silver](#3-data-transformation-layer--silver)
4. [Analytics Layer — Gold](#4-analytics-layer--gold)
5. [Orchestration and Monitoring](#5-orchestration-and-monitoring)
6. [Data Governance](#6-data-governance)
7. [Dashboard Design](#7-dashboard-design)
8. [Code Reference](#8-code-reference)

---

## 1. Pipeline Architecture Overview

### Design Philosophy

The pipeline is built on three core principles that informed every 
architectural decision:

**Production parity:** Every engineering pattern mirrors what a 
production data team would implement — header authentication, 
pagination handling, rate limit management, incremental ingestion, 
schema evolution, declarative transformations, and automated 
orchestration. The only difference from a live production deployment 
is the API tier — upgrading to a paid subscription activates current 
season data without any architectural changes.

**Separation of concerns:** Each layer has exactly one responsibility. 
The Bronze layer preserves raw data without modification. The Silver 
layer cleans and normalizes without business filtering. The Gold layer 
applies business logic without transformation. This strict separation 
ensures every layer is independently trustworthy and independently 
rerunnable.

**Parameterization over hardcoding:** The pipeline is designed to 
answer the same analytical question for any league, season, or 
nationality by changing configuration values — not code. 
`TARGET_LEAGUE_NAME`, `TARGET_SEASON`, `TARGET_LEAGUE_ID`, and 
`TARGET_NATIONALITY` are the only values that need to change to 
redirect the entire pipeline.

### Technology Selection

**Databricks** was selected as the unified platform because it 
consolidates every layer of the data engineering stack — ingestion, 
storage, transformation, orchestration, and visualization — into a 
single governed environment. This eliminates integration complexity 
between tools, ensures all data assets are managed under Unity Catalog 
governance from the moment they land, and allows the complete pipeline 
to be monitored, debugged, and maintained from one interface.

**API-Football** was selected over static CSV datasets specifically 
because it replicates a true production environment — it requires 
header authentication, returns deeply nested JSON that needs 
engineering work to flatten, enforces pagination that the pipeline 
must handle dynamically, and applies rate limiting that requires 
defensive programming techniques.

**Delta Lake** was selected as the storage format because it adds 
ACID transaction compliance, schema enforcement, schema evolution, 
and a complete transaction audit log on top of standard Parquet files 
— giving warehouse-level reliability at data lake storage costs.

**LakeFlow Declarative Pipelines** were selected for the Silver and 
Gold transformation layers because the declarative model — defining 
what each table IS rather than imperatively writing how to build it 
— allows LakeFlow to handle dependency resolution, infrastructure 
provisioning, parallel execution, and data quality monitoring 
automatically. The Bronze layer uses an imperative notebook because 
it performs external API calls, rate limiting, and file I/O operations 
that LakeFlow is not designed to handle.

### Medallion Architecture Implementation
```
SOURCE
API-Football REST API (api-sports.io)
        ↓
BRONZE — bronze_dev.api_sports
raw_serie_a_players_2024
→ Raw, untransformed API payload
→ Incremental append via Auto Loader
→ Schema evolution enabled
        ↓
SILVER — silver_dev.football_analytics
players_cleaned
→ Flattened, deduplicated, enriched
→ 607 active players, 43+ columns
→ 7 data quality expectations enforced
        ↓
GOLD — gold_dev.football_analytics
→ player_performance
→ player_ranking
→ team_nationality_contribution
→ position_performance_breakdown
→ league_nationality_summary
        ↓
DASHBOARD
Databricks SQL — Colombian Players in Serie A 2024
```

The Bronze layer is implemented as an imperative PySpark notebook. 
The Silver and Gold layers are implemented as a LakeFlow Declarative 
Pipeline. This boundary — imperative for ingestion, declarative for 
transformation — reflects the correct tool selection for each concern.

---

## 2. Data Extraction Layer — Bronze

### API Integration Strategy

The API-Football REST API exposes three endpoints consumed by the 
pipeline:

| Endpoint | Purpose | Calls Per Run |
|---|---|---|
| `/leagues` | Resolve league name to internal integer ID | 1 |
| `/teams` | Extract all team IDs for the target league and season | 1 |
| `/players` | Extract player statistics paginated by team | Up to 60 |

The `/leagues` and `/teams` endpoints serve as pre-flight validation — 
confirming that the configured league, country, and season exist in 
the API before consuming pagination quota. A `ValueError` with 
actionable guidance is raised immediately if either validation fails.

### Authentication and Rate Limit Handling

Every request includes the API key in the request header:
```python
HEADERS = {"x-apisports-key": API_KEY}
```

The free tier enforces a 10 requests per minute rate limit. A 
deliberate 7-second delay between every page request mathematically 
guarantees the pipeline executes approximately 8 requests per minute 
— safely within the limit regardless of response time variance.

### Team-Based Extraction Design

The free tier hard-caps league-wide pagination at page 3, blocking 
access to the majority of players when querying by league. The 
pipeline bypasses this restriction by querying by team instead — 
each team roster fits within 3 pages, making the per-team approach 
fully compatible with the free tier while retrieving the complete 
dataset.

This architectural decision is encoded in the extraction engine as a 
nested loop — an outer loop over the 20 validated team IDs and an 
inner loop handling dynamic pagination per team. The total pages per 
team is read from the API's own `paging.total` metadata field on the 
first page, making the pagination entirely dynamic rather than 
hardcoded.

The error handling within the extraction loop distinguishes between 
two failure categories:

**Hard limit errors** (e.g., free tier page cap): trigger a clean 
`break` to exit the current team's pagination and advance to the next 
team. Retrying is pointless — the constraint is structural, not 
transient.

**Transient errors** (e.g., rate limit spikes, network blips): trigger 
a 15-second pause and a `continue` to retry the exact same page. The 
data from that page is not lost.

### Distributed File Generation Pattern

Databricks Unity Catalog Volumes sit over immutable cloud object 
storage (AWS S3, Azure Data Lake, or GCS). Cloud objects cannot be 
appended to or modified in place — only written as complete new 
objects, read, or deleted. This constraint makes a single growing 
file impossible and makes the one-file-per-page pattern the only 
architecturally correct approach.

Every API page is serialized as a complete, immutable JSON file with 
a unique name combining team ID, page number, and run timestamp:
```
team_487_page_1_20260321_020000.json
team_487_page_2_20260321_020000.json
```

The timestamp suffix ensures every pipeline run generates filenames 
that Auto Loader has never seen before, guaranteeing incremental 
detection works correctly across all runs. Unity Catalog Volumes are 
POSIX-accessible via FUSE mount, allowing standard Python `open()` 
I/O to write directly to cloud object storage transparently.

This multi-file architecture also optimizes Spark read performance — 
PySpark's worker nodes read the entire folder of files concurrently 
in parallel rather than scanning a single large file sequentially.

### Incremental Ingestion with Auto Loader

Databricks Auto Loader (`cloudFiles`) provides incremental ingestion 
with three key capabilities working together:

**File tracking via checkpoint:** A hidden `_checkpoints` directory 
records every filename successfully committed to the Delta table. On 
each run, Auto Loader compares the landing zone contents against this 
record and processes only genuinely new files — never reprocessing 
historical data regardless of how many files have accumulated.

**Schema persistence:** The nested JSON schema is inferred on the 
first run and saved to the checkpoint directory. Subsequent runs load 
the saved schema instantly rather than re-inferring it from scratch, 
saving compute time and ensuring schema consistency across runs.

**`trigger(availableNow=True)`:** Processes all currently unprocessed 
files in a single optimized batch then shuts down immediately — 
combining streaming's incremental intelligence with batch's bounded 
execution and cost efficiency.

### Schema Evolution and Fault Tolerance

Two mechanisms handle API schema changes without pipeline failures:

**`mergeSchema: true`** handles additive changes (new columns added 
by the API). The Delta table schema expands automatically to 
accommodate the new column, populated for new records and null for 
historical records. The pipeline continues without interruption.

**`_rescued_data` column:** Auto Loader automatically provisions this 
quarantine column. Records with severe schema incompatibilities 
(type mutations, malformed JSON) are captured here as raw strings 
rather than dropped or causing pipeline failure. This guarantees 
100% data capture at the Bronze layer for later auditing.

### Idempotency Guarantees

The Bronze pipeline achieves idempotency at three levels:

**Infrastructure level:** All `CREATE` statements use `IF NOT EXISTS` 
— safe to rerun without side effects.

**File level:** Timestamped filenames ensure no two runs produce 
conflicting filenames. Reruns never overwrite previous files.

**Ingestion level:** Auto Loader's checkpoint ensures no file is 
committed to the Delta table twice, regardless of how many times the 
pipeline runs.

---

## 3. Data Transformation Layer — Silver

### LakeFlow Declarative Pipeline Design

The Silver layer is implemented as a LakeFlow Declarative Pipeline 
rather than a standard PySpark notebook. The `@dlt.table` decorator 
replaces all manual infrastructure provisioning — LakeFlow 
automatically creates and manages the `players_cleaned` Delta table, 
handles dependency resolution, provisions compute, and tracks lineage 
without any explicit orchestration code.

The `players_cleaned` function reads from Bronze via 
`spark.read.table(BRONZE_TABLE)` rather than `dlt.read()` because 
the Bronze table exists outside the LakeFlow pipeline boundary — 
written by the imperative ingestion notebook, not defined by a 
`@dlt.table` decorator. The Bronze → Silver dependency is resolved 
at the Workflows level (Task 2 depends on Task 1) rather than within 
LakeFlow's internal DAG.

`TARGET_LEAGUE_ID` is read from the LakeFlow pipeline configuration 
parameter pipeline.target_league_id — defined in the pipeline UI 
alongside `TARGET_NATIONALITY`. The default fallback is 135 (Serie A), 
confirmed by the Bronze layer's Cell 2 pre-flight validation output. 
Centralizing this in the pipeline UI means retargeting the pipeline 
to a different league requires only UI configuration changes — no 
code edits in the transformation files.

### Field Selection Rationale

Every field in the Bronze schema was evaluated against a single 
criterion: is this field analytically meaningful in any conceivable 
future use case, and is the data quality sufficient to include it? 
Whether a field is used in the current Gold tables is explicitly 
irrelevant — Silver is the canonical clean dataset for the domain, 
not a table optimized for today's specific business questions.

Fields were excluded for one of four reasons:

| Exclusion Reason | Examples |
|---|---|
| Non-analytical content | photo URL, logo URL, flag URL, squad number |
| API data quality issues | dribbles.past, penalty.commited, penalty.won (stored as string despite being numeric) |
| Structural redundancy | league.country (redundant with league.name) |
| Point-in-time snapshots | player.injured (boolean at extraction time, not season-level) |

Position-specific goalkeeper metrics (`goals.conceded`, `goals.saves`, 
`penalty.saved`, `penalty.scored`, `penalty.missed`) are included 
despite being null for all outfield players. Excluding them would 
make goalkeeper analytics impossible at the Gold layer without 
returning to Bronze and re-implementing all cleaning logic.

Physical profile metrics (`height_cm`, `weight_kg`) use 
`regexp_extract()` rather than direct casting because the API 
inconsistently formats these values — some records store "185 cm" 
while others store "185". Direct casting would silently produce null 
for the unit-suffixed format. The inconsistency is only detectable 
through full dataset execution, not static schema inspection.

### Data Quality Framework

Seven `@dlt.expect` decorators enforce data quality at the pipeline 
engine level, classified into three severity tiers:

**Critical — pipeline halts:**

`@dlt.expect_or_fail("valid_player_id", "player_id IS NOT NULL")`

A record without a player identifier cannot be deduplicated, joined, 
or ranked. Every downstream Gold table would be silently corrupted. 
Stopping the pipeline immediately is safer than allowing 
unidentifiable records to propagate.

**Moderate — record removed:**

`@dlt.expect_or_drop("active_players_only", "minutes_played > 0")`

Players registered in the squad but who never participated in a match 
carry zero analytical value. Including them inflates player counts 
and deflates position and nationality averages in Gold aggregations. 
482 records removed in the current dataset.

**Informational — warn and preserve:**

| Expectation | Rule | Rationale |
|---|---|---|
| valid_shot_consistency | shots_on_target ≤ shots_total | Mathematical impossibility |
| valid_dribble_consistency | dribbles_successful ≤ dribble_attempts | Mathematical impossibility |
| valid_duel_consistency | duels_won ≤ duels_total | Mathematical impossibility |
| valid_pass_accuracy | NULL OR between 0 and 100 | Percentage range validation |
| valid_minutes_if_appeared | appearances = 0 OR minutes_played > 0 | Logical consistency |

These rules reflect football domain logic rather than generic range 
checks. A goalkeeper with zero goals is completely normal. A player 
with more shots on target than total shots is a data impossibility 
regardless of position — this distinction between domain normality 
and logical impossibility makes the expectation set analytically 
meaningful.

### Normalization and Deduplication Strategy

The Bronze payload contains one row per player with a nested 
statistics array — one element per competition (Serie A, Coppa 
Italia, Champions League, etc.). `explode()` converts this into one 
row per player per competition, enabling league filtering. A null 
guard immediately after removes records where the statistics array 
is null or empty.

After flattening, `dropDuplicates(["player_id", "team_id", "season"])` 
collapses duplicate records produced by multiple Bronze pipeline runs. 
The composite key preserves mid-season transfer history — a player 
who moved clubs appears as two distinct rows, one per team.

### Null Handling Philosophy

Two categories of null values are treated differently:

**Cumulative counters** (goals, tackles, appearances): `fillna(0)` 
applied. A player who never scored has 0 goals — zero is the correct 
and meaningful value.

**Rate and percentage metrics** (rating, pass_accuracy_percent) and 
**physical attributes** (height_cm, weight_kg): left as NULL. A null 
rate means "not recorded" — semantically different from 0% which 
implies attempts were made and all failed. Filling with 0 would 
artificially deflate Gold layer averages.

### Derived Metrics Design

Five analytical rate metrics are computed in Silver rather than 
Gold for three reasons: the formula lives in one place, every Gold 
table reads the same pre-computed value making consistency 
mathematically guaranteed, and the computation runs once rather than 
being duplicated across every Gold consumer.

All metrics use `when()` guards against division by zero — a zero 
denominator produces NULL rather than 0%, because 0% implies attempts 
were made and all failed, which is semantically different from no 
attempts being made.

| Metric | Formula | Business Question |
|---|---|---|
| shot_accuracy_percent | shots_on_target / shots_total × 100 | How precise is this player's shooting? |
| dribble_success_rate | dribbles_successful / dribble_attempts × 100 | How effective in 1v1 situations? |
| duel_win_rate | duels_won / duels_total × 100 | How dominant in physical contests? |
| goal_conversion_rate | goals_total / shots_total × 100 | How clinical a finisher? |
| minutes_per_goal | minutes_played / goals_total | How frequently does this player score? |

---

## 4. Analytics Layer — Gold

### Table Design and Business Questions

Five Gold tables answer specific business questions about target 
nationality player performance. All tables are written to 
`gold_dev.football_analytics` and consume Silver via `dlt.read()` 
— LakeFlow automatically enforces that Silver completes before any 
Gold table runs and visualizes the complete dependency DAG.

| Table | Business Question | Notes |
|---|---|---|
| player_performance | Who are the target nationality players and what are their complete stats? | All Silver columns preserved, no threshold |
| player_ranking | Who performed best accounting for position? | 900+ min threshold, composite score |
| team_nationality_contribution | Which clubs benefited most? | Aggregated by team |
| position_performance_breakdown | Which positions excel? | 900+ min threshold |
| league_nationality_summary | What was the overall impact? | Single KPI row |

### Position-Aware Composite Scoring

A naive goal-contribution ranking systematically undervalues 
defenders and goalkeepers. The composite scoring system addresses 
three fundamental flaws:

**Scale inconsistency:** Raw metrics (goals, assists) and percentage 
metrics (pass accuracy, duel win rate) operate on different scales. 
Min-Max normalization brings all absolute metrics to a 0-100 scale 
before weighting, ensuring metrics contribute proportionally to their 
assigned weights rather than their raw magnitude.

**Double-counting:** `goal_contributions = goals + assists`. Including 
all three in a scoring formula counts goals and assists twice, 
artificially inflating attacker scores. The composite score uses 
normalized goals and assists independently.

**Position blindness:** Separate weight matrices per position reflect 
football domain reality:

| Metric | Attacker | Midfielder | Defender | Goalkeeper |
|---|---|---|---|---|
| Rating | 20% | 20% | 25% | 40% |
| Goals (normalized) | 35% | 15% | 0% | — |
| Assists (normalized) | 10% | 25% | 5% | — |
| Shot Accuracy | 15% | 5% | 0% | — |
| Pass Accuracy | 5% | 20% | 30% | 15% |
| Duel Win Rate | 5% | 10% | 40% | 10% |
| Dribble Success | 10% | 5% | 0% | — |
| Saves (normalized) | — | — | — | 35% |

A 900+ minute threshold prevents small sample size distortion — 
substitutes with inflated efficiency metrics from limited appearances 
are excluded from the ranking and position breakdown.

### Parameterization and Reusability

`TARGET_NATIONALITY` is read from a LakeFlow pipeline configuration 
parameter (`pipeline.target_nationality`) defined once in the pipeline 
UI. Both Silver and Gold read from this single source — changing the 
parameter value in the UI redirects the entire pipeline to any 
nationality without touching any code.

The same parameterization applies at the Bronze level — 
`TARGET_LEAGUE_NAME`, `TARGET_COUNTRY`, and `TARGET_SEASON` in Cell 1 
control the entire extraction. Changing these three values retargets 
the complete pipeline to any league and season.

### Module-Level Pre-computation Pattern

LakeFlow prohibits `collect()` operations inside `@dlt.table` 
decorated functions — it conflicts with the declarative execution 
model. The composite scoring system requires Min-Max normalization 
ranges computed from the actual player data, which requires 
`collect()`.

The solution is to compute all normalization ranges at module level 
before any decorated function executes:
```python
_df_stats = spark.read.table(SILVER_TABLE).filter(...)
_stats = _df_stats.agg(...).collect()[0]
MIN_GOALS = float(_stats["min_goals"] or 0)
...
del _df_stats, _stats
```

The constants (`MIN_GOALS`, `MAX_GOALS`, etc.) are then referenced 
inside the ranking function without any `collect()` call. Temporary 
DataFrames are deleted after computation to keep the module namespace 
clean.

A pre-flight validation check confirms at least one qualified player 
exists before computing ranges — raising a descriptive `ValueError` 
immediately if zero qualified players are found, rather than allowing 
a cryptic division-by-zero error to surface during ranking.

---

## 5. Orchestration and Monitoring

### Workflow Job Design

The complete pipeline is automated via a single Databricks Workflows 
job with two dependent tasks:

| Setting | Value |
|---|---|
| Job name | serie_a_colombian_players_daily_pipeline |
| Schedule | Daily at 03:00 CET |
| Notification | Email on failure only |
| Task 1 | Bronze ingestion notebook — Serverless compute |
| Task 2 | LakeFlow pipeline — Serverless compute |
| Dependency | Task 2 runs only if Task 1 succeeds |
| Retry policy | None on Task 1 (see below) |

### DAG Structure and Dependency Enforcement

The Workflow implements a linear DAG at the job level:
```
[Bronze Ingestion] ──Success──▶ [LakeFlow Pipeline]
                   ──Failure──▶ [Skipped]
```

Inside the LakeFlow pipeline a second DAG fans out from Silver to 
all five Gold tables in parallel:
```
[players_cleaned]
    ├──▶ [player_performance]
    ├──▶ [player_ranking]
    ├──▶ [team_nationality_contribution]
    ├──▶ [position_performance_breakdown]
    └──▶ [league_nationality_summary]
```

The fan-out pattern means total LakeFlow execution time is determined 
by the slowest Gold table rather than the sum of all five — maximizing 
parallelism within the declarative pipeline boundary.

When Bronze fails, the LakeFlow task enters `SKIPPED` state rather 
than `FAILED`. This distinction is operationally significant — 
`SKIPPED` means the task never ran because its dependency failed, 
immediately identifying that the problem originated in Bronze rather 
than in the transformation logic.

### Failure Handling Strategy

The Bronze task retry policy is deliberately set to `None` rather than 
the standard 2-retry configuration. Each Bronze run consumes 
approximately 62 of the 100 daily API requests. Two automatic retries 
on a failed run would consume up to 124 requests — exceeding the daily 
quota and leaving no capacity for the next scheduled run.

Failures trigger an immediate email notification. The engineer 
investigates the failure, waits for the quota to reset at midnight, 
and manually reruns when capacity is restored. This is the correct 
operational decision given the API constraint — automatic retries 
would solve transient failures but create a quota exhaustion problem 
that is harder to recover from.

### Observability Layers

The pipeline implements monitoring at four distinct levels:

**Infrastructure monitoring (Workflows):** Job run history records 
start time, end time, duration, and task status for every execution. 
Answers governance questions — did the pipeline run, did it succeed, 
how long did it take.

**Execution monitoring (print statements):** Every significant step 
in the Bronze notebook emits a descriptive print statement visible 
in the task logs. These statements serve as production execution logs 
— not just development debugging aids.

**Data quality monitoring (LakeFlow expectations):** After every 
pipeline run, the LakeFlow UI displays pass/fail counts for all seven 
data quality expectations. Catches data quality issues that allow the 
pipeline to technically succeed while producing analytically wrong 
data — which infrastructure monitoring alone cannot detect.

**Business metric monitoring (Dashboard):** The KPI panel and 
individual statistics table provide immediate visual sanity checking 
— an analyst scanning the dashboard spots business implausibilities 
that no automated rule would catch.

---

## 6. Data Governance

### Unity Catalog Hierarchy

All pipeline assets are organized under Unity Catalog's 3-level 
namespace (Catalog → Schema → Table/Volume):
```
bronze_dev (Catalog)
└── api_sports (Schema — named after source system)
    ├── landing_zone (Volume — raw JSON files)
    └── raw_serie_a_players_2024 (Delta Table)

silver_dev (Catalog)
└── football_analytics (Schema — named after business domain)
    └── players_cleaned (Delta Table)

gold_dev (Catalog)
└── football_analytics (Schema — named after business domain)
    ├── player_performance (Delta Table)
    ├── player_ranking (Delta Table)
    ├── team_nationality_contribution (Delta Table)
    ├── position_performance_breakdown (Delta Table)
    └── league_nationality_summary (Delta Table)
```

### Environment Separation

Three separate catalogs (`bronze_dev`, `silver_dev`, `gold_dev`) 
enforce strict environment separation between layers. In a production 
environment this pattern extends to `bronze_test`, `silver_test`, 
`gold_test` and `bronze_prod`, `silver_prod`, `gold_prod` — the same 
pipeline code runs against each environment with complete data 
isolation between them.

### Schema Naming Convention

Bronze schema is named `api_sports` — after the source system. This 
follows the convention that Bronze schemas identify where the data 
came from.

Silver and Gold schemas are named `football_analytics` — after the 
business domain. This follows the convention that transformation layer 
schemas identify what the data is for, not where it originated.

### Data Lineage

Delta Lake's `_delta_log` transaction log provides complete data 
lineage at the table level — every commit is recorded with timestamp, 
operation type, schema state, and record counts. LakeFlow's pipeline 
DAG provides lineage at the transformation level — showing exactly 
which Silver table feeds which Gold tables. Together these two 
mechanisms answer any audit question about data provenance.

### Credential Management

The API key is stored locally in a `.env` file excluded from version 
control via `.gitignore`. The production pattern — Databricks Secrets 
via `dbutils.secrets.get()` — is documented as a future improvement.

---

## 7. Dashboard Design

### Panel Architecture

The dashboard is structured to tell a progressive analytical story — 
headline summary first, sophisticated ranking second, supporting 
context third, complete detail last.

| Panel | Source Table | Visualization | Question Answered |
|---|---|---|---|
| Season Overview KPI | league_nationality_summary | Table (1 row) | Overall Colombian impact |
| Performance Ranking | player_ranking | Table | Who performed best by position? |
| Goal Contributions | player_performance | Bar Chart | Who scored and assisted? |
| Performance by Position | position_performance_breakdown | Bar Chart | Which positions excel? |
| Club Contribution | team_nationality_contribution | Bar Chart | Which clubs benefited most? |
| Individual Statistics | player_performance | Table | Complete player detail |

### Dataset Separation Principle

Every panel references exactly one Gold Delta table. No cross-dataset 
joins occur at the dashboard layer — all joining and aggregation logic 
is handled upstream in the Gold LakeFlow tables. The dashboard is a 
pure presentation layer with zero transformation logic.

### Threshold Consistency

The 900 minute threshold is applied consistently in the Performance 
Ranking and Position Breakdown panels where averages and composite 
scores could be distorted by small samples. The Goal Contributions 
chart and Individual Statistics table intentionally omit the threshold 
to preserve the complete player picture — D. Zapata and J. Cabal are 
visible in these panels despite being excluded from the ranking.

### Null Handling Display

Null values (notably D. Vásquez's shot accuracy) are displayed as 
null rather than 0 or N/A — consistent with the Silver layer's 
strategic null preservation. A null rate metric means the metric is 
not applicable to this player's role, not that the value is zero.

---

## 8. Code Reference

### Bronze Layer — Cell-by-Cell Breakdown

**Cell 1: Pipeline Configuration**
Imports, runtime parameters, and API endpoint definitions. The only 
cell that requires modification to target a different league, country, 
or season. All downstream cells inherit these variables automatically.

**Cell 2: Pre-Flight Metadata Validation**
Two lightweight API calls validate league, country, and season before 
consuming pagination quota. Raises `ValueError` with actionable 
guidance on invalid parameters. Dynamically resolves the integer 
league ID and extracts all team IDs — the extraction targets for 
Cell 4.

**Cell 3: Landing Zone Initialization**
Defines all path variables and ensures the run folder exists in the 
Unity Catalog Volume. Catalog, schema, and volume provisioning is 
handled by `01_infrastructure_setup.ipynb` — this cell only creates 
the run-specific folder within the existing Volume.

**Cell 4: Distributed Extraction Engine**
Nested loop over team IDs with dynamic pagination. Writes each API 
page as a uniquely timestamped immutable JSON file. Enforces 7-second 
delay between requests. Distinguishes hard limit errors (clean break) 
from transient errors (15-second pause and retry).

**Cell 5: Auto Loader Delta Commit**
Structured Streaming read with `cloudFiles` format. Schema inference 
on first run, schema loading from checkpoint on subsequent runs. 
`mergeSchema` for additive API changes. `_rescued_data` for malformed 
records. `trigger(availableNow=True)` for cost-efficient batch 
execution. `awaitTermination()` blocks until the async stream fully 
commits.

---

### silver_transformations.py

**Configuration:**
- `TARGET_NATIONALITY` — from LakeFlow pipeline parameter 
  `pipeline.target_nationality`
- `TARGET_LEAGUE_ID` — from LakeFlow pipeline parameter 
  pipeline.target_league_id. Default fallback: 135 (Serie A).
  Defined in the pipeline UI alongside TARGET_NATIONALITY.
- `BRONZE_TABLE` — fully qualified Bronze table path as named constant

**`@dlt.table` decorator:**
Defines `players_cleaned` in `silver_dev.football_analytics`. 
LakeFlow handles all infrastructure provisioning automatically.

**7 data quality expectations:**
3-tier classification — 1 pipeline-halting, 1 record-dropping, 
5 informational logical consistency checks.

**6-step transformation:**
Explode → Flatten & Standardize → Filter by League → Deduplicate → 
Null Handling → Derived Columns

---

### gold_transformations.py

**Configuration:**
- `TARGET_NATIONALITY` — from LakeFlow pipeline parameter 
  `pipeline.target_nationality`
- `SILVER_TABLE` — fully qualified Silver table path as named constant

**Module-level pre-computation:**
Normalization ranges computed before any decorated function executes 
to comply with LakeFlow's `collect()` restriction. Pre-flight 
validation raises `ValueError` if zero qualified players exist.

**5 Gold tables:**

*player_performance:* All Silver columns preserved. No minutes 
threshold. Source for goal contributions chart and individual 
statistics table.

*player_ranking:* Four-step process — minutes threshold (900+), 
Min-Max normalization, position-based composite scoring, 
`rank().over(window)` with composite score → rating → minutes 
tiebreaker chain.

*team_nationality_contribution:* `groupBy("team_name", "season")`. 
Total players, goals, assists, minutes, average rating and pass 
accuracy per club.

*position_performance_breakdown:* 900+ minute threshold applied 
before `groupBy("position", "season")`. Prevents small sample 
distortion in position-level averages.

*league_nationality_summary:* Single aggregation, no `groupBy`. 
One headline KPI row covering total players, goals, assists, minutes, 
average rating, pass accuracy, duel win rate, yellow cards, red cards.
