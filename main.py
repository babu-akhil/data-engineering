import pandas
import soccerdata as sd

from models.understat import (
    validate_and_insert_players,
    validate_and_insert_teams,
    validate_and_insert_understat_player_match_stats,
)

import configparser

# Create a ConfigParser object
config = configparser.ConfigParser()

# Read the config.ini file
config.read("config.ini")

# Access the FOOTBALL_DB section
db_config = config["FOOTBALL_DB"]

# Extract individual settings
dbname = db_config.get("dbname")
user = db_config.get("user")
password = db_config.get("password")
host = db_config.get("host")
port = db_config.get("port")

conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

print(conn_string)

understat = sd.Understat(leagues="ENG-Premier League", seasons="2023/2024")

player_match_stats = understat.read_player_match_stats().reset_index()
validate_and_insert_understat_player_match_stats(player_match_stats, conn_string)
