## import packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import nba_api
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.static import players
from nba_api.stats.library.parameters import SeasonAll
from nba_api.stats.static import teams as static_teams
from nba_api.stats.static import players as static_players
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import leaguedashplayerstats
import time
import requests
import json
import seaborn as sns
import sys
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()



### first lets pull the team IDs from the NBA API
def get_team_ids_df():
    teams_list = static_teams.get_teams()
    teams_df = pd.DataFrame(teams_list).sort_values("full_name")
    teams_df.rename(columns={'id': 'team_id'}, inplace=True)
    ## add a date time processed column
    teams_df["date_time_processed"] = pd.Timestamp.utcnow()
    teams_df["date_time_processed"] = teams_df["date_time_processed"].astype(str)
    return teams_df

## get a dictionary of team IDs to team info
def get_team_id():
    teams_list = static_teams.get_teams()
    TEAM_BY_ID   = {t["id"]: t for t in teams_list}
    return TEAM_BY_ID

### next lets pull the player IDs from the NBA API
def get_player_df():
    players_list = static_players.get_players()
    players_df = pd.DataFrame(players_list)
    players_df.rename(columns={'id': 'player_id'}, inplace=True)
    ## add a date time processed column
    players_df["date_time_processed"] = pd.Timestamp.utcnow()
    players_df["date_time_processed"] = players_df["date_time_processed"].astype(str)
    return players_df

## pull in team games 
def get_team_games_df(teams_df):
    team_games = [] 
    for t in teams_df['team_id']:
        games_df = leaguegamefinder.LeagueGameFinder(team_id_nullable=t).get_data_frames()[0]
        # add a date time processed column
        games_df["date_time_processed"] = pd.Timestamp.utcnow()
        games_df["date_time_processed"] = games_df["date_time_processed"].astype(str)
        team_games.append(games_df)

    team_games_df = pd.concat(team_games)
    return team_games_df

## prepare team_games_data for supabase upload
def prepare_team_games_data_for_supabase(team_games_df):
    fact_team_games_df = team_games_df.rename(columns={
        "SEASON_ID": "season_id",
        "TEAM_ID": "team_id",
        "GAME_ID": "game_id",
        "GAME_DATE": "game_date",
        "MATCHUP": "matchup",
        "WL": "wl",
        "PTS": "pts",
        "FG_PCT": "fg_pct",
        "FG3_PCT": "fg3_pct",
        "FT_PCT": "ft_pct",
        "REB": "reb",
        "AST": "ast",
        "STL": "stl",
        "BLK": "blk",
        "TOV": "tov",
        "PLUS_MINUS": "plus_minus",
        "DATE_TIME_PROCESSED": "date_time_processed"
    })

    # Step 2: define the exact column order your Supabase table uses
    supabase_col_order = [
        "game_id",
        "team_id",
        "game_date",
        "matchup",
        "wl",
        "pts",
        "plus_minus",
        "season_id",
        "fg_pct",
        "fg3_pct",
        "ft_pct",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "date_time_processed"
    ]

    # Step 3: restrict to those columns and reorder
    fact_team_games_df = fact_team_games_df[supabase_col_order]

    # Step 4: convert dates to ISO format (Supabase/Postgres friendly)
    fact_team_games_df["game_date"] = (
        fact_team_games_df["game_date"].astype("datetime64[ns]").dt.date
    )

    # Ensure game_date is a datetime type
    fact_team_games_df["game_date"] = pd.to_datetime(fact_team_games_df["game_date"]).dt.date
    
    # Define cutoff
    cutoff_date = pd.to_datetime("2025-12-03").date()

    # Filter rows
    fact_team_games_df = fact_team_games_df[fact_team_games_df["game_date"] < cutoff_date]

    # 1) Ensure datetime, then convert to ISO string
    fact_team_games_df["game_date"] = (
        pd.to_datetime(fact_team_games_df["game_date"])
        .dt.strftime("%Y-%m-%d")   # now plain strings like "2025-11-12"
    )

    # 2) (Recommended) Replace NaN/NaT with None so JSON can handle them
    fact_team_games_df = fact_team_games_df.replace({np.nan: None})

    ## drop duplicates in the raw data
    fact_team_games_df = fact_team_games_df.drop_duplicates(
        subset=["game_id", "team_id"],
        keep="first"
    ).reset_index(drop=True)
    
    return fact_team_games_df

## pull in player season stats
def get_player_season_stats_df():
    SEASONS = ["2020-21","2021-22","2022-23","2023-24", "2024-25"]
    player_season_dfs = []

    for SEASON in SEASONS:
        print(f"Processing season: {SEASON}")
        player_season_df = leaguedashplayerstats.LeagueDashPlayerStats(
            season=SEASON,
            season_type_all_star="Regular Season",   # or "Playoffs"
            per_mode_detailed="PerGame",            # "Totals", "Per36", "Per100Possessions", etc.
            measure_type_detailed_defense="Base",       # "Base", "Advanced", "Misc", "Scoring", etc.
        ).get_data_frames()[0]
        print(f"Season {SEASON} data shape: {player_season_df.shape}")
        player_season_df['season'] = SEASON
        # add date time processed column
        player_season_df['date_time_processed'] = pd.Timestamp.utcnow()
        player_season_df["date_time_processed"] = player_season_df["date_time_processed"].astype(str)
        
        player_season_dfs.append(player_season_df)

    player_season_df_compiled = pd.concat(player_season_dfs)

    player_season_df_compiled["PLAYER_NAME"] = player_season_df_compiled["PLAYER_NAME"].astype(str)
    player_season_df_compiled["NICKNAME"] = player_season_df_compiled["NICKNAME"].astype(str)
    player_season_df_compiled["TEAM_ABBREVIATION"] = player_season_df_compiled["TEAM_ABBREVIATION"].astype(str)

    return player_season_df_compiled

## prepare player season stats for supabase upload
def prepare_player_season_stats_for_supabase(player_season_df_compiled):
    rename_map = {
        "PLAYER_ID": "player_id",
        "PLAYER_NAME": "player_name",
        "NICKNAME": "nickname",
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbreviation",
        "AGE": "age",
        "GP": "gp",
        "W": "w",
        "L": "l",
        "W_PCT": "w_pct",
        "MIN": "min",
        "FGM": "fgm",
        "FGA": "fga",
        "FG_PCT": "fg_pct",
        "FG3M": "fg3m",
        "FG3A": "fg3a",
        "FG3_PCT": "fg3_pct",
        "FTM": "ftm",
        "FTA": "fta",
        "FT_PCT": "ft_pct",
        "OREB": "oreb",
        "DREB": "dreb",
        "REB": "reb",
        "AST": "ast",
        "TOV": "tov",
        "STL": "stl",
        "BLK": "blk",
        "BLKA": "blka",
        "PF": "pf",
        "PFD": "pfd",
        "PTS": "pts",
        "PLUS_MINUS": "plus_minus",
        "NBA_FANTASY_PTS": "nba_fantasy_pts",
        "DD2": "dd2",
        "TD3": "td3",
        "WNBA_FANTASY_PTS": "wnba_fantasy_pts",
        "GP_RANK": "gp_rank",
        "W_RANK": "w_rank",
        "L_RANK": "l_rank",
        "W_PCT_RANK": "w_pct_rank",
        "MIN_RANK": "min_rank",
        "FGM_RANK": "fgm_rank",
        "FGA_RANK": "fga_rank",
        "FG_PCT_RANK": "fg_pct_rank",
        "FG3M_RANK": "fg3m_rank",
        "FG3A_RANK": "fg3a_rank",
        "FG3_PCT_RANK": "fg3_pct_rank",
        "FTM_RANK": "ftm_rank",
        "FTA_RANK": "fta_rank",
        "FT_PCT_RANK": "ft_pct_rank",
        "OREB_RANK": "oreb_rank",
        "DREB_RANK": "dreb_rank",
        "REB_RANK": "reb_rank",
        "AST_RANK": "ast_rank",
        "TOV_RANK": "tov_rank",
        "STL_RANK": "stl_rank",
        "BLK_RANK": "blk_rank",
        "BLKA_RANK": "blka_rank",
        "PF_RANK": "pf_rank",
        "PFD_RANK": "pfd_rank",
        "PTS_RANK": "pts_rank",
        "PLUS_MINUS_RANK": "plus_minus_rank",
        "NBA_FANTASY_PTS_RANK": "nba_fantasy_pts_rank",
        "DD2_RANK": "dd2_rank",
        "TD3_RANK": "td3_rank",
        "WNBA_FANTASY_PTS_RANK": "wnba_fantasy_pts_rank",
        "TEAM_COUNT": "team_count",
        "SEASON": "season",
        "DATE_TIME_PROCESSED": "date_time_processed"
    }

    player_season_df_compiled = player_season_df_compiled.rename(columns=rename_map)

    return player_season_df_compiled

## connect to supabase
def connect_to_supabase():
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

## upsert in chunks
def upsert_in_chunks(table_name, records, chunk_size=500):
    supabase = connect_to_supabase()
    total_records = len(records)
    for start in range(0, total_records, chunk_size):
        end = start + chunk_size
        chunk = records[start:end]
        print(f"Upserting records {start} to {end} into {table_name}...")
        response = supabase.table(table_name).upsert(chunk).execute()
        if response.status_code != 200:
            print(f"Error upserting chunk {start} to {end}: {response.data}")
        else:
            print(f"Successfully upserted records {start} to {end}.")
    return "upset complete"

## run the main ETL process
def run_etl_process():
    # Step 1: Get team IDs
    teams_df = get_team_ids_df()

    # Step 2: Get team games data
    team_games_df = get_team_games_df(teams_df)

    # Step 3: Prepare team games data for Supabase
    fact_team_games_df = prepare_team_games_data_for_supabase(team_games_df)

    # Step 4: Upsert team games data into Supabase
    fact_team_games_records = fact_team_games_df.to_dict(orient="records")
    upsert_in_chunks("fact_team_games", fact_team_games_records, chunk_size=500)

    # Step 5: Get player season stats data
    player_season_df_compiled = get_player_season_stats_df()

    # Step 6: Prepare player season stats data for Supabase
    player_season_df_prepared = prepare_player_season_stats_for_supabase(player_season_df_compiled)

    # Step 7: Upsert player season stats data into Supabase
    player_season_records = player_season_df_prepared.to_dict(orient="records")
    upsert_in_chunks("fact_player_season_stats", player_season_records, chunk_size=500)

    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl_process()