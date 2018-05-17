PLAYER_COLUMNS_RENAME = {
    'clearances_blocks_interceptions': 'cbi',
    'element': 'player_id',
    'fixture': 'match_id',
    'opponent_team': 'team_o_id', # N.B. we only have opposition team available here, will get team later from fixtures table 
    'total_points': 'points', 
    'round': 'gw',
    'missed_target': 'shots_off_target' 
}

PLAYER_HISTORY_COLUMNS_RENAME = {
    'total_points': 'points'
}


H2H_LEAGUE_COLUMNS_RENAME = {
    'event': 'gw',
    'entry_1_player_name': 'name',
    'entry_1_points': 'points',
    'entry_1_total': 'h2h_points',
    'entry_2_player_name': 'name_o',
    'entry_2_points': 'points_o',
    'entry_2_total': 'h2h_points_o'
}


USER_COLUMNS_RENAME = {
    'entry': 'user_id',
    'event': 'gw',
    'event_transfers': 'gw_transfers',
    'event_transfers_cost': 'gw_transfers_cost',
}

USER_PICKS_COLUMNS_RENAME = {
    'element': 'player_id',
}
   

PLAYER_COLUMNS_DROP = {
    'ea_index',
    'ict_index',
    'id',
    'kickoff_time',
    'kickoff_time_formatted',
    'was_home',
    'team_a_score',
    'team_h_score'
}


PLAYER_HISTORY_COLUMNS_DROP = {
    'ea_index',
    'ict_index',
    'id', 
    'element_code', 
    'season'
}


H2H_LEAGUE_COLUMNS_KEEP = {
    'gw',
    'result',
    'name',
    'points',
    'points_o',
    'name_o',
    'h2h_points'
} 


USER_COLUMNS_DROP = {
    'movement',
    'id',
    'targets'
}


TEAM_NAME_SHORT = {
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