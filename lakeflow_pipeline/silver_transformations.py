# =============================================================================
# FILE: silver_transformations.py
# PURPOSE: Defines the players_cleaned Silver Delta table using LakeFlow's
#          declarative @dlt.table decorator.
# INPUT:   bronze_dev.api_sports.raw_serie_a_players_2024 (Bronze Delta Table)
# OUTPUT:  silver_dev.football_analytics.players_cleaned (Silver Delta Table)
# =============================================================================

import dlt
from pyspark.sql.functions import col, explode, trim, lower, when, regexp_extract

# Shared with gold_transformations.py via pipeline UI parameter
TARGET_NATIONALITY = spark.conf.get("pipeline.target_nationality", "colombia")

# Filters exploded statistics to target league only — confirmed by Bronze Cell 2 output
TARGET_LEAGUE_ID = int(spark.conf.get("pipeline.target_league_id", "135"))

# Update when targeting a different league or season
BRONZE_TABLE = "bronze_dev.api_sports.raw_serie_a_players_2024"

# =============================================================================
# SILVER TABLE: players_cleaned
# =============================================================================

@dlt.table(
    name="players_cleaned",
    comment="Flattened, deduplicated, and enriched player statistics. One row per player per team per season."
)

# CRITICAL: Pipeline halts — null player_id corrupts all downstream Gold tables
@dlt.expect_or_fail("valid_player_id", "player_id IS NOT NULL")

# MODERATE: Ghost players removed — zero analytical value, distorts averages
@dlt.expect_or_drop("active_players_only", "minutes_played > 0")

# INFORMATIONAL: Logical impossibilities — warn and preserve
@dlt.expect("valid_shot_consistency",    "shots_on_target <= shots_total")
@dlt.expect("valid_dribble_consistency", "dribbles_successful <= dribble_attempts")
@dlt.expect("valid_duel_consistency",    "duels_won <= duels_total")
@dlt.expect("valid_pass_accuracy",       "pass_accuracy_percent IS NULL OR (pass_accuracy_percent >= 0 AND pass_accuracy_percent <= 100)")
@dlt.expect("valid_minutes_if_appeared", "appearances = 0 OR minutes_played > 0")

def players_cleaned():

    df_bronze = spark.read.table(BRONZE_TABLE)

    # --- STEP 1: EXPLODE ---
    # Statistics array has one entry per competition — explode enables league filtering
    df_exploded = df_bronze.withColumn(
        "stat_element", explode(col("statistics"))
    )
    df_exploded = df_exploded.filter(col("stat_element").isNotNull())

    # --- STEP 2: FLATTEN & STANDARDIZE ---
    # Reference/filter columns: trim + lower for consistent filtering
    # Display columns: trim only — preserve natural casing for dashboard
    # height/weight: regexp_extract handles API inconsistency ("185 cm" vs "185")
    df_flattened = df_exploded.select(

        # PLAYER IDENTITY
        col("player.id").alias("player_id"),
        trim(col("player.name")).alias("player_name"),
        trim(col("player.firstname")).alias("first_name"),
        trim(col("player.lastname")).alias("last_name"),
        col("player.age").cast("integer").alias("age"),
        regexp_extract(col("player.height"), r"(\d+)", 1)
            .cast("integer").alias("height_cm"),
        regexp_extract(col("player.weight"), r"(\d+)", 1)
            .cast("integer").alias("weight_kg"),
        trim(lower(col("player.nationality"))).alias("nationality"),

        # TEAM & LEAGUE
        col("stat_element.team.id").alias("team_id"),
        trim(lower(col("stat_element.team.name"))).alias("team_name"),
        col("stat_element.league.id").alias("league_id"),
        trim(lower(col("stat_element.league.name"))).alias("league_name"),
        col("stat_element.league.season").cast("integer").alias("season"),

        # APPEARANCES & TIME
        col("stat_element.games.appearences").cast("integer").alias("appearances"),
        col("stat_element.games.lineups").cast("integer").alias("lineups"),
        col("stat_element.games.minutes").cast("integer").alias("minutes_played"),
        col("stat_element.substitutes.in").cast("integer").alias("sub_in"),
        col("stat_element.substitutes.out").cast("integer").alias("sub_out"),
        trim(lower(col("stat_element.games.position"))).alias("position"),
        col("stat_element.games.rating").cast("float").alias("rating"),

        # ATTACK
        col("stat_element.goals.total").cast("integer").alias("goals_total"),
        col("stat_element.goals.assists").cast("integer").alias("assists_total"),
        col("stat_element.shots.total").cast("integer").alias("shots_total"),
        col("stat_element.shots.on").cast("integer").alias("shots_on_target"),

        # PENALTIES
        col("stat_element.penalty.scored").cast("integer").alias("penalties_scored"),
        col("stat_element.penalty.missed").cast("integer").alias("penalties_missed"),

        # PASSING
        col("stat_element.passes.total").cast("integer").alias("passes_total"),
        col("stat_element.passes.key").cast("integer").alias("key_passes"),
        col("stat_element.passes.accuracy").cast("integer").alias("pass_accuracy_percent"),

        # DRIBBLING
        col("stat_element.dribbles.attempts").cast("integer").alias("dribble_attempts"),
        col("stat_element.dribbles.success").cast("integer").alias("dribbles_successful"),

        # DUELS
        col("stat_element.duels.total").cast("integer").alias("duels_total"),
        col("stat_element.duels.won").cast("integer").alias("duels_won"),

        # DEFENDING
        col("stat_element.tackles.total").cast("integer").alias("tackles_total"),
        col("stat_element.tackles.interceptions").cast("integer").alias("interceptions"),
        col("stat_element.tackles.blocks").cast("integer").alias("blocks"),

        # FOULS & DISCIPLINE
        col("stat_element.fouls.committed").cast("integer").alias("fouls_committed"),
        col("stat_element.fouls.drawn").cast("integer").alias("fouls_drawn"),
        col("stat_element.cards.yellow").cast("integer").alias("yellow_cards"),
        col("stat_element.cards.red").cast("integer").alias("red_cards"),

        # GOALKEEPER
        col("stat_element.goals.conceded").cast("integer").alias("goals_conceded"),
        col("stat_element.goals.saves").cast("integer").alias("saves"),
        col("stat_element.penalty.saved").cast("integer").alias("penalties_saved"),
    )

    # --- STEP 3: FILTER BY LEAGUE ---
    # Other competitions (Coppa Italia, UCL) became separate rows after explode — discard here.
    # Ghost player removal handled by @dlt.expect_or_drop, not here.
    df_league_only = df_flattened.filter(
        col("league_id") == TARGET_LEAGUE_ID
    )

    # --- STEP 4: DEDUPLICATE ---
    # Composite key preserves mid-season transfer history — one row per player per team.
    df_deduplicated = df_league_only.dropDuplicates(
        ["player_id", "team_id", "season"]
    )

    # --- STEP 5: NULL HANDLING ---
    # Cumulative counters → fillna(0): zero is the correct value for no activity
    # Rate metrics (rating, pass_accuracy_percent) → NULL: not recorded ≠ zero
    # Physical attributes (height_cm, weight_kg) → NULL: unknown ≠ zero
    cumulative_stats = [
        "appearances", "lineups", "minutes_played", "sub_in", "sub_out",
        "goals_total", "assists_total", "shots_total", "shots_on_target",
        "penalties_scored", "penalties_missed",
        "passes_total", "key_passes",
        "dribble_attempts", "dribbles_successful",
        "duels_total", "duels_won",
        "tackles_total", "interceptions", "blocks",
        "fouls_committed", "fouls_drawn",
        "yellow_cards", "red_cards",
        "goals_conceded", "saves", "penalties_saved"
    ]

    df_clean = df_deduplicated.fillna(0, subset=cumulative_stats)

    # --- STEP 6: DERIVED COLUMNS ---
    # Computed once in Silver — Gold tables read pre-computed values,
    # guaranteeing consistency across all consumers.
    # when() guards division by zero — zero denominator produces NULL,
    # not 0%, because no attempts ≠ zero success rate.
    df_final = df_clean.select(
        "*",
        when(col("shots_total") > 0,
            (col("shots_on_target") / col("shots_total") * 100)
            .cast("float")).alias("shot_accuracy_percent"),

        when(col("dribble_attempts") > 0,
            (col("dribbles_successful") / col("dribble_attempts") * 100)
            .cast("float")).alias("dribble_success_rate"),

        when(col("duels_total") > 0,
            (col("duels_won") / col("duels_total") * 100)
            .cast("float")).alias("duel_win_rate"),

        when(col("shots_total") > 0,
            (col("goals_total") / col("shots_total") * 100)
            .cast("float")).alias("goal_conversion_rate"),

        when(col("goals_total") > 0,
            (col("minutes_played") / col("goals_total"))
            .cast("float")).alias("minutes_per_goal"),
    )

    return df_final