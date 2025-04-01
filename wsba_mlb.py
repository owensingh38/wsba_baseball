import requests as rs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import random
from tools.mlb_scraping import *
import warnings
warnings.filterwarnings('ignore')

### WSBA BASEBALL ###

### SCRAPE FUNCTIONS ###
def mlb_scrape_game(game_ids):
    #Given a set of game ids, return complete play-by-play data
    # param 'game_ids' - MLB game ids

    pbps = []
    for game_id in game_ids:
        print("Scraping data from game " + str(game_id) + "...",end="")
        start = time.perf_counter()

        game_id = str(game_id)

        api = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"

        #Retrieve raw data
        try: 
            json = rs.get(api).json()
            #Parse JSONs and HTMLs
            data = parse_json(json)
                
            #Append data to list
            no_data = False
            pbps.append(data)
            
            end = time.perf_counter()
            secs = end - start
            print(f" finished in {secs:.2f} seconds.")

        except FileNotFoundError:
            no_data = True
            print(f"No data found for game {game_id}...")
            
        
    if no_data:
        return pd.DataFrame()
    else:
        #Add all pbps together
        df = pd.concat(pbps, ignore_index=True)

    #Return: complete play-by-play
    return df

def mlb_scrape_schedule(season,start = "02/01", end = "12/01"):
    #Given a season, return schedule data
    # param 'season' - MLB season to scrape
    # param 'start' - Start date in season
    # param 'end' - End date in season

    api = "https://statsapi.mlb.com/api/v1/schedule/games/?sportId=1&date="

    form = '%m/%d/%Y'

    #Set start and end
    start = start+"/"+str(season)
    end = end+"/"+str(season)

    #Create datetime values from dates
    start = datetime.strptime(start,form)
    end = datetime.strptime(end,form)

    game = []

    day = (end-start).days+1
    if day < 0:
        #Handles dates which are over a year apart
        day = 365 + day
    for i in range(day):
        #For each day, call MLB api and retreive id, season, season_type (1,2,3), and gamecenter link
        inc = start+timedelta(days=i)
        month = f'{inc}'[5:7]
        day = f'{inc}'[8:10]
        year = f'{inc}'[0:4]

        date = f'{month}/{day}/{year}'
        print(f"Scraping games on {date}...")

        get = rs.get(f'{api}{date}').json()
        gameWeek = get['dates'][0]['games'] if len(get['dates'])>0 else []

        for i in range(len(gameWeek)):
            game.append(pd.DataFrame({
                "id": [gameWeek[i]['gamePk']],
                "season": [gameWeek[i]['season']],
                "season_type":[gameWeek[i]['gameType']],
                "away_team_abbr":[gameWeek[i]['teams']['away']['team']['name']],
                "home_team_abbr":[gameWeek[i]['teams']['home']['team']['name']],
                "api_feed_link":[gameWeek[i]['link']]
                }))
    
    #Concatenate all games
    df = pd.concat(game)
    
    #Return: specificed schedule data
    return df

def mlb_scrape_season(season, season_types = ['R','P'], start = "02/01", end = "12/01", local=False, local_path = "schedule/schedule.csv"):
    #Given season, scrape all play-by-play occuring within the season
    # param 'season' - MLB season to scrape
    # param 'start' - Start date in season
    # param 'end' - End date in season
    # param 'local' - boolean indicating whether to use local file to scrape game_ids
    # param 'local_path' - path of local file

    #Determine whether to use schedule data in repository or to scrape
    if local == True:
        load = pd.read_csv(local_path)
        load = load.loc[(load['season'].astype(str)==str(season))&(load['season_type'].isin(season_types))]
        game_ids = list(load['id'].astype(str))
    else:
        load = mlb_scrape_schedule(season,start,end)
        load = load.loc[(load['season'].astype(str)==str(season))&(load['season_type'].isin(season_types))]
        game_ids = list(load['id'].astype(str))

    #If no games found, terminate the process
    if not game_ids:
        print('No games found for dates in season...')
        return ""
    
    print(f"Scraping games from {season} season...")
    start = time.perf_counter()
    
    #Perform scrape
    data = mlb_scrape_game(game_ids)
    
    end = time.perf_counter()
    secs = end - start

    print(f'Finished season scrape in {(secs/60)/60:.2f} hours.')

    #Return: Complete pbp and shifts data for specified season as well as dataframe of game_ids which failed to return data
    return data

mlb_scrape_season(2024,season_types=['R']).to_csv("mlb_pbp_2024.csv",index=False)