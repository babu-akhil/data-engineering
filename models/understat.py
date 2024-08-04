import pandas as pd
from sqlalchemy import create_engine


def validate_and_insert_players(df, conn_string):
    """
    Validates a pandas DataFrame to ensure it can be inserted into the players table,
    and then inserts it using df.to_sql.

    Parameters:
    df (pd.DataFrame): The DataFrame to validate and insert.
    conn_string (str): The connection string for the database.

    Returns:
    None
    """
    required_columns = {"understat_id", "player_name"}

    # Check if all required columns are present
    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"DataFrame must contain the following columns: {required_columns}"
        )

    # Check for non-empty player_name
    if df["player_name"].str.len().min() <= 0:
        raise ValueError("All player_name values must be non-empty")

    # Check for unique understat_id
    if df["understat_id"].duplicated().any():
        raise ValueError("understat_id values must be unique within the DataFrame")

    # Create a SQLAlchemy engine
    engine = create_engine(conn_string)

    # Insert the DataFrame into the players table
    df.to_sql("players", con=engine, if_exists="append", index=False)


def validate_and_insert_teams(df, conn_string):
    """
    Validates a pandas DataFrame to ensure it can be inserted into the teams table,
    and then inserts it using df.to_sql.

    Parameters:
    df (pd.DataFrame): The DataFrame to validate and insert.
    conn_string (str): The connection string for the database.

    Returns:
    None
    """
    required_columns = {"understat_id", "team_name"}

    # Check if all required columns are present
    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"DataFrame must contain the following columns: {required_columns}"
        )

    # Check for non-empty team_name
    if df["team_name"].str.len().min() <= 0:
        raise ValueError("All team_name values must be non-empty")

    # Check for unique understat_id
    if df["understat_id"].duplicated().any():
        raise ValueError("understat_id values must be unique within the DataFrame")

    # Check for unique team_name
    if df["team_name"].duplicated().any():
        raise ValueError("team_name values must be unique within the DataFrame")

    # Create a SQLAlchemy engine
    engine = create_engine(conn_string)

    # Insert the DataFrame into the teams table
    df.to_sql("teams", con=engine, if_exists="append", index=False)


def validate_and_insert_understat_player_match_stats(df, conn_string):
    """
    Validates a pandas DataFrame to ensure it can be inserted into the understat_player_match_stats table,
    and then inserts it using df.to_sql.

    Parameters:
    df (pd.DataFrame): The DataFrame to validate and insert.
    conn_string (str): The connection string for the database.

    Returns:
    None
    """
    required_columns = {
        "game_id",
        "league_id",
        "team_id",
        "season_id",
        "player_id",
        "player",
        "team",
        "game",
        "season",
        "league",
        "position",
        "minutes",
        "goals",
        "assists",
        "shots",
        "key_passes",
        "xg",
        "xa",
        "xg_chain",
        "xg_buildup",
        "own_goals",
        "yellow_cards",
        "red_cards",
    }

    df = df[list(required_columns)]

    # Check if all required columns are present
    if not required_columns.issubset(df.columns):
        raise ValueError(
            f"DataFrame must contain the following columns: {required_columns}"
        )

    # Check for non-empty string columns
    non_empty_columns = ["player", "team", "game", "league", "position"]
    for col in non_empty_columns:
        if df[col].str.len().min() <= 0:
            raise ValueError(f"All {col} values must be non-empty")

    # Create a SQLAlchemy engine
    engine = create_engine(conn_string)
    # Insert the DataFrame into the understat_player_match_stats table
    df.to_sql(
        "understat_player_match_stats", con=engine, if_exists="append", index=False
    )
