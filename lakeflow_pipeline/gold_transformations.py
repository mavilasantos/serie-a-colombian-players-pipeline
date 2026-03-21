# =============================================================================
# FILE: gold_transformations.py
# PURPOSE: Defines five analytical Gold Delta tables using LakeFlow's
#          declarative @dlt.table decorators, consuming the Silver
#          players_cleaned table via dlt.read().
# INPUT:   silver_dev.football_analytics.players_cleaned (Silver Delta Table)
# OUTPUT:  gold_dev.football_analytics.player_performance
#          gold_dev.football_analytics.player_ranking
#          gold_dev.football_analytics.team_nationality_contribution
#          gold_dev.football_analytics.position_performance_breakdown
#          gold_dev.football_analytics.league_nationality_summary
# =============================================================================

import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col, avg, count, round, rank, desc,
    coalesce, lit, when
)
from pyspark.sql.window import Window

# Shared with silver_transformations.py via pipeline UI parameter
TARGET_NATIONALITY = spark.conf.get("pipeline.target_nationality", "colombia")

# Update if Silver catalog or schema changes
SILVER_TABLE = "silver_dev.football_analytics.players_cleaned"

# =============================================================================
# MODULE-LEVEL PRE-COMPUTATION
# =============================================================================
# collect() prohibited inside @dlt.table — ranges computed here as constants

_df_stats = spark.read.table(SILVER_TABLE).filter(
    (col("nationality") == TARGET_NATIONALITY) &
    (col("minutes_played") >= 900)
)

# Fail fast — descriptive error if no qualified players exist for the
# configured nationality and threshold before any decorated function runs
_qualified_count = _df_stats.count()

if _qualified_count == 0:
    raise ValueError(
        f"PIPELINE ABORTED: No qualified players found for "
        f"nationality='{TARGET_NATIONALITY}' with minutes_played >= 900. "
        f"Possible causes: "
        f"(1) TARGET_NATIONALITY is misspelled — expected lowercase (e.g. 'colombia'). "
        f"(2) No players of this nationality played 900+ minutes in the target league/season. "
        f"(3) Silver table '{SILVER_TABLE}' is empty or stale. "
        f"Action: Verify the pipeline.target_nationality parameter in the pipeline UI "
        f"and rerun Bronze and Silver before retrying."
    )

del _qualified_count

_stats = _df_stats.agg(
    F.min("goals_total").alias("min_goals"),
    F.max("goals_total").alias("max_goals"),
    F.min("assists_total").alias("min_assists"),
    F.max("assists_total").alias("max_assists"),
    F.min("saves").alias("min_saves"),
    F.max("saves").alias("max_saves"),
).collect()[0]

MIN_GOALS   = float(_stats["min_goals"]   or 0)
MAX_GOALS   = float(_stats["max_goals"]   or 1)
MIN_ASSISTS = float(_stats["min_assists"] or 0)
MAX_ASSISTS = float(_stats["max_assists"] or 1)
MIN_SAVES   = float(_stats["min_saves"]   or 0)
MAX_SAVES   = float(_stats["max_saves"]   or 1)

# Range guards — prevent division by zero if all players share identical values
GOALS_RANGE   = MAX_GOALS   - MIN_GOALS   if MAX_GOALS   != MIN_GOALS   else 1.0
ASSISTS_RANGE = MAX_ASSISTS - MIN_ASSISTS if MAX_ASSISTS != MIN_ASSISTS else 1.0
SAVES_RANGE   = MAX_SAVES   - MIN_SAVES   if MAX_SAVES   != MIN_SAVES   else 1.0

del _df_stats, _stats

# =============================================================================
# GOLD TABLE 1: player_performance
# =============================================================================

@dlt.table(
    name="player_performance",
    comment=f"Complete season statistics for {TARGET_NATIONALITY} players. All Silver columns preserved."
)
@dlt.expect("valid_nationality_filter", f"nationality = '{TARGET_NATIONALITY}'")
def player_performance():
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .orderBy(desc("goals_total"), desc("assists_total"))
    )

# =============================================================================
# GOLD TABLE 2: player_ranking
# =============================================================================
# Position-aware composite scoring addresses three flaws in naive rankings:
#   1. Scale inconsistency — Min-Max normalization brings all metrics to 0-100
#   2. Double-counting — goal_contributions excluded, goals/assists used independently
#   3. Position blindness — separate weight matrices per position

@dlt.table(
    name="player_ranking",
    comment=f"Position-aware composite score ranking for {TARGET_NATIONALITY} players with 900+ minutes."
)
def player_ranking():

    df = dlt.read("players_cleaned").filter(
        col("nationality") == TARGET_NATIONALITY
    )

    # 900+ minutes — prevents small sample size distortion from substitutes
    df_qualified = df.filter(col("minutes_played") >= 900)

    # Min-Max normalization — pre-computed ranges avoid collect() restriction
    df_normalized = df_qualified.withColumn(
        "norm_goals",
        ((col("goals_total") - MIN_GOALS) / GOALS_RANGE * 100).cast("float")
    ).withColumn(
        "norm_assists",
        ((col("assists_total") - MIN_ASSISTS) / ASSISTS_RANGE * 100).cast("float")
    ).withColumn(
        # Saves only meaningful for goalkeepers — outfield players receive 0
        "norm_saves",
        when(
            col("position") == "goalkeeper",
            ((col("saves") - MIN_SAVES) / SAVES_RANGE * 100).cast("float")
        ).otherwise(lit(0))
    )

    # coalesce() defaults null rate metrics to 0 for scoring only —
    # original null values in Silver are unaffected
    r  = coalesce(col("rating"), lit(0))
    sa = coalesce(col("shot_accuracy_percent"), lit(0))
    pa = coalesce(col("pass_accuracy_percent"), lit(0))
    dw = coalesce(col("duel_win_rate"), lit(0))
    dr = coalesce(col("dribble_success_rate"), lit(0))
    ng = coalesce(col("norm_goals"), lit(0))
    na = coalesce(col("norm_assists"), lit(0))
    ns = coalesce(col("norm_saves"), lit(0))

    # Weight matrices reflect position-specific football value
    # Full rationale in TECHNICAL_DOCUMENTATION.md Section 4
    attacker_score = (
        (r * 0.20) + (ng * 0.35) + (na * 0.10) +
        (sa * 0.15) + (pa * 0.05) + (dw * 0.05) + (dr * 0.10)
    ).cast("float")

    midfielder_score = (
        (r * 0.20) + (ng * 0.15) + (na * 0.25) +
        (sa * 0.05) + (pa * 0.20) + (dw * 0.10) + (dr * 0.05)
    ).cast("float")

    defender_score = (
        (r * 0.25) + (ng * 0.00) + (na * 0.05) +
        (sa * 0.00) + (pa * 0.30) + (dw * 0.40) + (dr * 0.00)
    ).cast("float")

    goalkeeper_score = (
        (r * 0.40) + (ns * 0.35) +
        (pa * 0.15) + (dw * 0.10)
    ).cast("float")

    df_scored = df_normalized.withColumn(
        "composite_score",
        when(col("position") == "attacker",   attacker_score)
        .when(col("position") == "midfielder", midfielder_score)
        .when(col("position") == "defender",   defender_score)
        .when(col("position") == "goalkeeper", goalkeeper_score)
        .otherwise(lit(0))
    )

    # Tiebreaker: rating → minutes_played
    window = Window.orderBy(
        desc("composite_score"),
        desc("rating"),
        desc("minutes_played")
    )

    return df_scored.select(
        rank().over(window).alias("performance_rank"),
        "player_name",
        "team_name",
        "position",
        "appearances",
        "minutes_played",
        "goals_total",
        "assists_total",
        (col("goals_total") + col("assists_total")).alias("goal_contributions"),
        F.round(col("composite_score"), 2).alias("composite_score"),
        F.round(col("rating"), 2).alias("rating"),
        # Normalized values preserved for engineering transparency —
        # excluded from dashboard display
        F.round(col("norm_goals"), 1).alias("normalized_goals"),
        F.round(col("norm_assists"), 1).alias("normalized_assists"),
        F.round(col("duel_win_rate"), 1).alias("duel_win_rate"),
        F.round(col("pass_accuracy_percent"), 1).alias("pass_accuracy_percent"),
        F.round(col("shot_accuracy_percent"), 1).alias("shot_accuracy_percent"),
        F.round(col("dribble_success_rate"), 1).alias("dribble_success_rate"),
    ).orderBy("performance_rank")

# =============================================================================
# GOLD TABLE 3: team_nationality_contribution
# =============================================================================

@dlt.table(
    name="team_nationality_contribution",
    comment=f"Aggregated contribution of {TARGET_NATIONALITY} players per club."
)
def team_nationality_contribution():
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .groupBy("team_name", "season")
        .agg(
            count("player_id").alias("total_players"),
            F.sum("goals_total").alias("total_goals"),
            F.sum("assists_total").alias("total_assists"),
            F.sum("minutes_played").alias("total_minutes"),
            F.round(avg("rating"), 2).alias("avg_rating"),
            F.round(avg("pass_accuracy_percent"), 2).alias("avg_pass_accuracy")
        )
        .orderBy(desc("total_goals"))
    )

# =============================================================================
# GOLD TABLE 4: position_performance_breakdown
# =============================================================================
# 900+ minute threshold applied for consistency with player_ranking —
# position-level averages would be distorted by small sample sizes

@dlt.table(
    name="position_performance_breakdown",
    comment=f"Performance by position for {TARGET_NATIONALITY} players with 900+ minutes."
)
def position_performance_breakdown():
    return (
        dlt.read("players_cleaned")
        .filter(
            (col("nationality") == TARGET_NATIONALITY) &
            (col("minutes_played") >= 900)
        )
        .groupBy("position", "season")
        .agg(
            count("player_id").alias("total_players"),
            F.sum("goals_total").alias("total_goals"),
            F.sum("assists_total").alias("total_assists"),
            F.round(avg("minutes_played"), 0).alias("avg_minutes_played"),
            F.round(avg("rating"), 2).alias("avg_rating"),
            F.round(avg("duel_win_rate"), 2).alias("avg_duel_win_rate"),
            F.round(avg("shot_accuracy_percent"), 2).alias("avg_shot_accuracy")
        )
        .orderBy("position")
    )

# =============================================================================
# GOLD TABLE 5: league_nationality_summary
# =============================================================================

@dlt.table(
    name="league_nationality_summary",
    comment=f"Single row KPI summary of {TARGET_NATIONALITY} player presence across the full league season."
)
def league_nationality_summary():
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .agg(
            count("player_id").alias("total_players"),
            F.sum("goals_total").alias("total_goals"),
            F.sum("assists_total").alias("total_assists"),
            F.sum("minutes_played").alias("total_minutes_played"),
            F.round(avg("rating"), 2).alias("avg_player_rating"),
            F.round(avg("pass_accuracy_percent"), 2).alias("avg_pass_accuracy"),
            F.round(avg("duel_win_rate"), 2).alias("avg_duel_win_rate"),
            F.sum("yellow_cards").alias("total_yellow_cards"),
            F.sum("red_cards").alias("total_red_cards")
        )
    )