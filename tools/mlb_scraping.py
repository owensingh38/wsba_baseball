import pandas as pd
import numpy as np
import requests as rs
import warnings
warnings.filterwarnings('ignore')

### SCRAPING FUNCTIONS ###

def get_col():
    return[ 'game_id','game_date','season','season_type','double_header',
            'away_team_abbr','home_team_abbr','at_bat_id','pitch_num','inning',
            'start_time','end_time','away_score','home_score',
            'balls','strikes','outs','result_type','scoring_play','reviewed',
            'event','event_type','description','call_code','call','in_play',
            'ball','strike','out','pitch_type','base_running_play','from_catcher',
            'runner_going','substitution',
            'batter_name','batter_id','pitcher_name','pitcher_id',
            'origin_base', 'start_base', 'end_base', 'out_base', 'out_num', 'unearned',
            'event_player_1_name','event_player_2_name',
            'event_player_3_name','event_player_4_name',
            'event_player_1_id','event_player_2_id',
            'event_player_3_id','event_player_4_id',
            'event_player_1_pos','event_player_2_pos',
            'event_player_3_pos','event_player_4_pos',
            'event_player_1_credit','event_player_2_credit',
            'event_player_3_credit','event_player_4_credit',
            'responsible_pitcher_id',
            'pitch_start_speed', 'pitch_end_speed', 
            'pitch_strike_zone_top', 'pitch_strike_zone_bottom', 
            'pitch_aX', 'pitch_aY', 'pitch_aZ', 
            'pitch_pfxX', 'pitch_pfxZ', 
            'pitch_pX', 'pitch_pZ', 
            'pitch_vX0', 'pitch_vY0', 'pitch_vZ0', 
            'pitch_x', 'pitch_y', 
            'pitch_x0', 'pitch_y0', 'pitch_z0', 
            'pitch_break_angle', 'pitch_break_length', 
            'pitch_break_Y', 'pitch_break_vertical', 'pitch_break_vertical_induced', 
            'pitch_break_horizontal', 'pitch_spin_rate', 'pitch_zone', 'pitch_type_confidence',
            'pitch_plate_time', 'pitch_extension', 
            'hit_launch_speed', 'hit_launch_angle', 
            'hit_distance', 'hit_trajectory', 
            'hit_hardness', 'hit_location', 
            'hit_x', 'hit_y'
            ]

def parse_json(json):
    #Given raw json data, return parsed play-by-play data
    
    #Split into four parts - info, teams, rosters, plays
    info = json['gameData']
    teams = json['gameData']['teams']
    rosters = json['gameData']['players']
    plays = json['liveData']['plays']['allPlays']

    if not plays:
        raise FileNotFoundError

    #Build roster information
    #For some reason player info is divided into unique keys based on their id
    name = {}
    pos = {}
    for player in rosters:
        name.update({rosters[player]['id']:[rosters[player]['fullName'].upper()]})
        pos.update({rosters[player]['id']:[rosters[player]['primaryPosition']['abbreviation']]})

    #Plays include all at-bats in a provided match, with individual pitch and runner data included for each event
    events = []
    

    #Columns to assess
    col = ['startTime','endTime','type','details.description',
           'details.eventType','details.homeScore','details.awayScore',
                'details.isScoringPlay',
                'details.isOut',
                'details.hasReview',
                'count.balls',
                'count.strikes',
                'count.outs',
                'player.id',
                'pitchNumber',
                'details.call.code',
                'details.call.description',
                'details.isInPlay',
                'details.isStrike',
                'details.isBall',
                'details.type.description',
                'pitchData.startSpeed',
                'pitchData.endSpeed',
                'pitchData.strikeZoneTop',
                'pitchData.strikeZoneBottom',
                'pitchData.coordinates.aX',
                'pitchData.coordinates.aY',
                'pitchData.coordinates.aZ',
                'pitchData.coordinates.pfxX',
                'pitchData.coordinates.pfxZ',
                'pitchData.coordinates.pX',
                'pitchData.coordinates.pZ',
                'pitchData.coordinates.vX0',
                'pitchData.coordinates.vY0',
                'pitchData.coordinates.vZ0',
                'pitchData.coordinates.x',
                'pitchData.coordinates.y',
                'pitchData.coordinates.x0',
                'pitchData.coordinates.y0',
                'pitchData.coordinates.z0',
                'pitchData.breaks.breakAngle',
                'pitchData.breaks.breakLength',
                'pitchData.breaks.breakY',
                'pitchData.breaks.breakVertical',
                'pitchData.breaks.breakVerticalInduced',
                'pitchData.breaks.breakHorizontal',
                'pitchData.breaks.spinRate',
                'pitchData.zone',
                'pitchData.typeConfidence',
                'pitchData.plateTime',
                'pitchData.extension',
                'hitData.launchSpeed',
                'hitData.launchAngle',
                'hitData.totalDistance',
                'hitData.trajectory',
                'hitData.hardness',
                'hitData.location',
                'hitData.coordinates.coordX',
                'hitData.coordinates.coordY',
                'isBaseRunningPlay',
                'details.fromCatcher',
                'details.runnerGoing',
                'isSubstitution',
                'position.abbreviation',
                'replacedPlayer.id',
                'movement.originBase', 
                'movement.start', 
                'movement.end', 
                'movement.outBase', 
                'movement.isOut', 
                'movement.outNumber', 
                'details.eventType', 
                'details.movementReason', 
                'details.runner.id', 
                'details.runner.fullName', 
                'details.responsiblePitcher.id', 
                'details.isScoringEvent', 
                'details.earned',
                'details.teamUnearned']
    
    for event in plays:
        #Format individual events (from each play)
        data = pd.json_normalize(event['playEvents'])
        
        #Check col
        for c in col:
            try: data[c]
            except: data[c]=''

        #Event Result and Info
        data['at_bat_id'] = event['about']['atBatIndex']
        data['inning'] = event['about']['inning']
        data['result_type'] = event['result']['eventType']
        
        data = data.rename(columns={
                'startTime':'start_time',
                'endTime':'end_time',
                'type':'event',
                'details.description':'description',
                'details.eventType':'event_type',
                'details.homeScore':'home_score',
                'details.awayScore':'away_score',
                'details.isScoringPlay':'scoring_play',
                'details.isOut':'out',
                'details.hasReview':'reviewed',
                'count.balls':'balls',
                'count.strikes':'strikes',
                'count.outs':'outs',
                'player.id':'event_player_1_id',
                'pitchNumber':'pitch_num',
                'details.call.code':'call_code',
                'details.call.description':'call',
                'details.isInPlay':'in_play',
                'details.isStrike':'strike',
                'details.isBall':'ball',
                'details.type.description':'pitch_type',
                'pitchData.startSpeed':'pitch_start_speed',
                'pitchData.endSpeed':'pitch_end_speed',
                'pitchData.strikeZoneTop':'pitch_strike_zone_top',
                'pitchData.strikeZoneBottom':'pitch_strike_zone_bottom',
                'pitchData.coordinates.aX':'pitch_aX',
                'pitchData.coordinates.aY':'pitch_aY',
                'pitchData.coordinates.aZ':'pitch_aZ',
                'pitchData.coordinates.pfxX':'pitch_pfxX',
                'pitchData.coordinates.pfxZ':'pitch_pfxZ',
                'pitchData.coordinates.pX':'pitch_pX',
                'pitchData.coordinates.pZ':'pitch_pZ',
                'pitchData.coordinates.vX0':'pitch_vX0',
                'pitchData.coordinates.vY0':'pitch_vY0',
                'pitchData.coordinates.vZ0':'pitch_vZ0',
                'pitchData.coordinates.x':'pitch_x',
                'pitchData.coordinates.y':'pitch_y',
                'pitchData.coordinates.x0':'pitch_x0',
                'pitchData.coordinates.y0':'pitch_y0',
                'pitchData.coordinates.z0':'pitch_z0',
                'pitchData.breaks.breakAngle':'pitch_break_angle',
                'pitchData.breaks.breakLength':'pitch_break_length',
                'pitchData.breaks.breakY':'pitch_break_Y',
                'pitchData.breaks.breakVertical':'pitch_break_vertical',
                'pitchData.breaks.breakVerticalInduced':'pitch_break_vertical_induced',
                'pitchData.breaks.breakHorizontal':'pitch_break_horizontal',
                'pitchData.breaks.spinRate':'pitch_spin_rate',
                'pitchData.zone':'pitch_zone',
                'pitchData.typeConfidence':'pitch_type_confidence',
                'pitchData.plateTime':'pitch_plate_time',
                'pitchData.extension':'pitch_extension',
                'hitData.launchSpeed':'hit_launch_speed',
                'hitData.launchAngle':'hit_launch_angle',
                'hitData.totalDistance':'hit_distance',
                'hitData.trajectory':'hit_trajectory',
                'hitData.hardness':'hit_hardness',
                'hitData.location':'hit_location',
                'hitData.coordinates.coordX':'hit_x',
                'hitData.coordinates.coordY':'hit_y',
                'isBaseRunningPlay':'base_running_play',
                'details.fromCatcher':'from_catcher',
                'details.runnerGoing':'runner_going',
                'isSubstitution':'substitution',
                'position.abbreviation':'event_player_2_pos',
                'replacedPlayer.id':'event_player_2_id'
            }
        )

        #Base running events
        run = pd.json_normalize(event['runners']).rename(columns={
            "movement.originBase":'origin_base',
            'movement.start':'start_base',
            'movement.end':'end_base',
            'movement.outBase':'out_base',
            'movement.isOut':'out',
            'movement.outNumber':'out_num',
            'details.eventType':'event_type',
            'details.movementReason':'description',
            'details.runner.id':'event_player_1_id',
            'details.responsiblePitcher.id':'responsible_pitcher_id',
            'details.isScoringEvent':'scoring_play',
            'details.earned':'earned',
            'details.teamUnearned':'unearned'
        })
        
        run['event_player_1_credit'] = 'runner'
        #Add other event players (usually fielder(s) who completed play)
        #Variable i iterates through runners while j iterates through each credited player for a runner
        for i in range(len(run['credits'])):
            #Some empty lists here are set with float value NaN
            try:
                for j in range(len(run['credits'][i])):
                    run[f'event_player_{j+2}_id'] = ''
                    run.at[i,f'event_player_{j+2}_id'] = run['credits'][i][j]['player']['id']
                    run[f'event_player_{j+2}_credit'] = ''
                    run.at[i,f'event_player_{j+2}_credit'] = run['credits'][i][j]['credit']
            except TypeError: 
                continue

        #Add other info
        run['at_bat_id'] = event['about']['atBatIndex']

        #Combine events
        atbat = pd.concat([data,run])

        #Apply batter and pitcher for at-bat
        atbat['batter_id'] = event['matchup']['batter']['id']
        atbat['batter_name'] = event['matchup']['batter']['fullName'].upper()
        atbat['event_player_1_hand'] = event['matchup']['batSide']['code']
        atbat['pitcher_id'] = event['matchup']['pitcher']['id']
        atbat['pitcher_name'] = event['matchup']['pitcher']['fullName'].upper()
        atbat['event_player_2_hand'] = event['matchup']['pitchHand']['code']

        #Add at-bat to game
        events.append(atbat)

    pbp = pd.concat(events).reset_index()

    #Add game info
    pbp['game_id'] = info['game']['pk']
    pbp['game_date'] = info['datetime']['dateTime']
    pbp['season'] = info['game']['season']
    pbp['season_type'] = info['game']['type']
    pbp['double_header'] = info['game']['doubleHeader']
    pbp['away_team_abbr'] = teams['away']['abbreviation']
    pbp['home_team_abbr'] = teams['home']['abbreviation']

    #Add player info
    for i in range(1,4):
        pbp[f'event_player_{i}_name'] = pbp[f'event_player_{i}_id'].replace(name)
        pbp[f'event_player_{i}_pos'] = pbp[f'event_player_{i}_id'].replace(pos)
    
    #Forward fill score
    score = ['away_score','home_score']
    for col in score:
        pbp[col] = pbp[col].replace(r'^\s*$', np.nan, regex=True).ffill()
    pbp['pitch_num'] = pbp['pitch_num'].fillna(0)

    col = [c for c in get_col() if c in pbp.columns.tolist()]
    #Return: complete play-by-play for provided game
    return pbp[col]