import configparser
import pandas
import soccerdata as sd

understat = sd.Understat(leagues="ENG-Premier League", seasons="2023/2024")
print(understat.__doc__)

leagues = understat.read_leagues()
leagues.head()

player_match_stats = understat.read_player_match_stats()
player_match_stats.head()