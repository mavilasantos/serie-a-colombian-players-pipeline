import dlt
from pyspark.sql.functions import (
    col, sum, avg, count, round, rank, desc
)
from pyspark.sql.window import Window

# =============================================================================
# CONFIGURATION
# =============================================================================
# Change TARGET_NATIONALITY to analyze any nationality in the league
# Must match the lowercased nationality values in the Silver table
TARGET_NATIONALITY = "colombia"

# =============================================================================
# GOLD TABLE 1: player_performance
# Purpose: Full season statistics for players of target nationality
# Source:  silver_dev.football_analytics.players_cleaned
# Output:  gold_dev.football_analytics.player_performance
# =============================================================================

@dlt.table(
    name="player_performance",
    comment=f"Complete 2024 Serie A season statistics for {TARGET_NATIONALITY} players. One row per player per team."
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
# Purpose: Performance ranking of target nationality players
# Source:  silver_dev.football_analytics.players_cleaned
# Output:  gold_dev.football_analytics.player_ranking
# =============================================================================

@dlt.table(
    name="player_ranking",
    comment=f"Performance ranking of {TARGET_NATIONALITY} players by combined goal contributions"
)

def player_ranking():
    window = Window.orderBy(
        desc(col("goals_total") + col("assists_total"))
    )
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .select(
            "player_name",
            "team_name",
            "position",
            "appearances",
            "minutes_played",
            "goals_total",
            "assists_total",
            (col("goals_total") + col("assists_total")).alias("goal_contributions"),
            "shot_accuracy_percent",
            "dribble_success_rate",
            "duel_win_rate",
            "pass_accuracy_percent",
            "rating",
            rank().over(window).alias("performance_rank")
        )
    )

# =============================================================================
# GOLD TABLE 3: team_nationality_contribution
# Purpose: Which teams have the highest contribution from target nationality
# Source:  silver_dev.football_analytics.players_cleaned
# Output:  gold_dev.football_analytics.team_nationality_contribution
# =============================================================================

@dlt.table(
    name="team_nationality_contribution",
    comment=f"Aggregated contribution of {TARGET_NATIONALITY} players per Serie A team"
)

def team_nationality_contribution():
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .groupBy("team_name", "season")
        .agg(
            count("player_id").alias("total_players"),
            sum("goals_total").alias("total_goals"),
            sum("assists_total").alias("total_assists"),
            sum("minutes_played").alias("total_minutes"),
            round(avg("rating"), 2).alias("avg_rating"),
            round(avg("pass_accuracy_percent"), 2).alias("avg_pass_accuracy")
        )
        .orderBy(desc("total_goals"))
    )

# =============================================================================
# GOLD TABLE 4: position_performance_breakdown
# Purpose: Performance breakdown by position for target nationality
# Source:  silver_dev.football_analytics.players_cleaned
# Output:  gold_dev.football_analytics.position_performance_breakdown
# =============================================================================

@dlt.table(
    name="position_performance_breakdown",
    comment=f"Performance breakdown by position for {TARGET_NATIONALITY} players"
)

def position_performance_breakdown():
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .groupBy("position", "season")
        .agg(
            count("player_id").alias("total_players"),
            sum("goals_total").alias("total_goals"),
            sum("assists_total").alias("total_assists"),
            round(avg("minutes_played"), 0).alias("avg_minutes_played"),
            round(avg("rating"), 2).alias("avg_rating"),
            round(avg("duel_win_rate"), 2).alias("avg_duel_win_rate"),
            round(avg("shot_accuracy_percent"), 2).alias("avg_shot_accuracy")
        )
        .orderBy("position")
    )

# =============================================================================
# GOLD TABLE 5: league_nationality_summary
# Purpose: Single row overall summary of target nationality in the league
# Source:  silver_dev.football_analytics.players_cleaned  
# Output:  gold_dev.football_analytics.league_nationality_summary
# =============================================================================

@dlt.table(
    name="league_nationality_summary",
    comment=f"Single row summary of {TARGET_NATIONALITY} player presence and performance in the league"
)

def league_nationality_summary():
    return (
        dlt.read("players_cleaned")
        .filter(col("nationality") == TARGET_NATIONALITY)
        .agg(
            count("player_id").alias("total_players"),
            sum("goals_total").alias("total_goals"),
            sum("assists_total").alias("total_assists"),
            sum("minutes_played").alias("total_minutes_played"),
            round(avg("rating"), 2).alias("avg_player_rating"),
            round(avg("pass_accuracy_percent"), 2).alias("avg_pass_accuracy"),
            round(avg("duel_win_rate"), 2).alias("avg_duel_win_rate"),
            sum("yellow_cards").alias("total_yellow_cards"),
            sum("red_cards").alias("total_red_cards")
        )
    )