PLAYER_RENAME = {
    'clearances_blocks_interceptions': 'cbi',
    'element': 'player_id',
    'fixture': 'match_id',
    'opponent_team': 'team_id_o', # N.B. we only have opposition team available here, will get team later from fixtures table 
    'total_points': 'points', 
    'round': 'gw',
    'missed_target': 'shots_off_target' 
}

PLAYER_HISTORY_RENAME = {
    'total_points': 'points'
}


H2H_LEAGUE_RENAME = {
    'event': 'gw',
    'entry_1_player_name': 'user_name',
    'entry_1_points': 'points',
    'entry_1_total': 'h2h_points',
    'entry_2_player_name': 'user_name_o',
    'entry_2_points': 'points_o',
    'entry_2_total': 'h2h_points_o'
}


USER_RENAME = {
    'entry': 'user_id',
    'event': 'gw',
    'event_transfers': 'gw_transfers',
    'event_transfers_cost': 'gw_transfers_cost',
}

USER_PICKS_RENAME = {
    'element': 'player_id',
}


MATCH_HOME_RENAME = {
    'team_h': 'team_id',
    'team_a': 'team_id_o',
    'team_h_score': 'score',
    'team_a_score': 'score_o',
}

MATCH_AWAY_RENAME = {
    'team_a': 'team_id',
    'team_h': 'team_id_o',
    'team_a_score': 'score',
    'team_h_score': 'score_o',
}
   

PLAYER_DROP = {
    'ea_index',
    'ict_index',
    'id',
    'kickoff_time',
    'kickoff_time_formatted',
    'was_home',
    'team_a_score',
    'team_h_score'
}


PLAYER_HISTORY_DROP = {
    'ea_index',
    'ict_index',
    'id', 
    'element_code', 
    'season'
}


H2H_LEAGUE_KEEP = {
    'gw',
    'result',
    'user_name',
    'points',
    'points_o',
    'user_name_o',
    'h2h_points'
} 


MATCH_DROP = {
    'id',
    'started',
    'event_day',
    'deadline_time',
    'deadline_time_formatted',
    'stats',
    'code',
    'kickoff_time_formatted',
    'finished',
    'minutes',
    'provisional_start_time',
    'finished_provisional',
    'event'
} 


USER_DROP = {
    'movement',
    'id',
    'targets'
}

TEAM_ID = {
    1: 'ars',
    2: 'bou',
    3: 'bha',
    4: 'bur',
    5: 'che',
    6: 'cpl',
    7: 'eve',
    8: 'hud',
    9: 'lei',
    10: 'liv',
    11: 'mci',
    12: 'mun',
    13: 'new',
    14: 'sot',
    15: 'stk',
    16: 'swa',
    17: 'tot',
    18: 'wat',
    19: 'wba',
    20: 'whu'
}


TEAM_NAME = {
    'arsenal': 'ars',
    'brighton': 'bha', 
    'bournemouth': 'bou', 
    'burnley': 'bur', 
    'chelsea': 'che',
    'crystal palace': 'cpl',
    'palace': 'cpl',
    'everton': 'eve', 
    'huddersfield': 'hud',
    'leicester': 'lei', 
    'liverpool': 'liv', 
    'manchester united': 'mun',
    'man united': 'mun', 
    'manchester city': 'mci',
    'man city': 'mci', 
    'newcastle': 'new', 
    'southampton': 'sot', 
    'spurs': 'tot',
    'tottenham': 'tot',
    'stoke city': 'stk', 
    'swansea': 'swa', 
    'watford': 'wat', 
    'west bromwich albion': 'wba',
    'west brom': 'wba', 
    'west ham': 'whu' 
}