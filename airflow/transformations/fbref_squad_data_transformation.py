import pandas as pd
import glob
import os


def _squad_data_transformation():
    files = glob.glob("/opt/airflow/data/squad_data*.csv")
    result = pd.DataFrame()
    keepers_result = pd.DataFrame()
    for file in files:
        file_name = os.path.basename(file)
        if file_name == "squad_data_defense.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Tkl': 'players_tackled',
                                    'TklW': 'tackles_won',
                                    'Def 3rd': 'tackles_in_defensive_1_3',
                                    'Mid 3rd': 'tackles_in_middle_1_3',
                                    'Att 3rd': 'tackles_in_attacking_1_3',
                                    'Tkl.1': 'dribbles_tackles',
                                    'Att': 'dribbled_past_plus_tackles',
                                    'Tkl%': 'percentage_of_dribbles_tackles',
                                    'Past': 'dribbles_pass_by_an_opposing_player',
                                    'Blocks': 'blocking_the_ball',
                                    'Sh': 'blocking_a_shot',
                                    'Pass': 'blocking_a_pass',
                                    'Int': 'interceptions',
                                    'Tkl+Int': 'players_tackled_plus_interceptions',
                                    'Clr': 'clearances',
                                    'Err': 'mistakes_leading_to_an_opponents_shot'},
                           inplace=True)

            result = pd.DataFrame(content)

        if file_name == "squad_data_gca.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'SCA': 'shot_creating_action',
                                    'SCA90': 'shot_creating_action_per_90_min',
                                    'PassLive': 'pass_live_passes_to_shot_attempt',
                                    'PassDead': 'dead_ball_passes_to_shot_attempt',
                                    'Drib': 'dribbles_to_shot_attempt',
                                    'Sh': 'shots_to_another_shot_attempt',
                                    'Fld': 'fouls_drawn_to_shot_attempt',
                                    'Def': 'defensive_actions_to_shot_attempt',
                                    'GCA': 'goal_creating_actions',
                                    'GCA90': 'goal_creating_actions_per_90_min',
                                    'PassLive.1': 'live_ball_passes_to_goal',
                                    'PassDead.1': 'dead_ball_passes_to_goal',
                                    'Drib.1': 'dribbles_to_goal',
                                    'Sh.1': 'shots_to_another_goal_scoring_shot',
                                    'Fld.1': 'fouls_drawn_to_goal',
                                    'Def.1': 'defensive_action_to_goal'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_misc.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'CrdY': 'yellow_cards',
                                    'CrdR': 'red_cards',
                                    '2CrdY': 'second_yellow_card',
                                    'Fls': 'fouls_committed',
                                    'Fld': 'fouls_drawn',
                                    'Off': 'offside',
                                    'Crs': 'crosses',
                                    'Int': 'interceptions',
                                    'TklW': 'tackles_won',
                                    'PKwon': 'penalty_kicks_won',
                                    'PKcon': 'penalty_kicks_conceded',
                                    'OG': 'own_goals',
                                    'Recov': 'loss_ball_recover',
                                    'Won': 'aerials_won',
                                    'Lost': 'aerials_lost',
                                    'Won%': 'percentage_aerials_won'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_passing.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Cmp': 'passes_completed',
                                    'Att': 'passes_attempted',
                                    'Cmp%': 'pass_completion_percent',
                                    'TotDist': 'total_distance_in_yards',
                                    'PrgDist': 'progressive_distance',
                                    'Cmp.1': 'passes_completed_5_15_yards',
                                    'Att.1': 'passes_attempted_5_15_yards',
                                    'Cmp%.1': 'pass_completion_percent_5_15_yards',
                                    'Cmp.2': 'passes_completed_15_30_yards',
                                    'Att.2': 'passes_attempted_15_30_yards',
                                    'Cmp%.2': 'pass_completion_percent_15_30_yards',
                                    'Cmp.3': 'passes_completed_longer_30_yards',
                                    'Att.3': 'passes_attempted_longer_30_yards',
                                    'Cmp%.3': 'pass_completion_percent_longer_30_yards',
                                    'Ast': 'assists',
                                    'xAG': 'expected_assisted_goals',
                                    'xA': 'expected_assists',
                                    'A-xAG': 'assists_minus_expected_goals_assisted',
                                    'KP': 'assisted_shots',
                                    '1/3': 'passes_enter_1_3_pitch_closest_to_goal',
                                    'PPA': 'passes_18_yard_box',
                                    'CrsPA': 'crosses_18_yard_box',
                                    'Prog': 'progressive_passes'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_passing_types.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Att': 'passes_attempted',
                                    'Live': 'live_ball_passes',
                                    'Dead': 'dead_ball_passes',
                                    'FK': 'passes_attempted_from_free_kicks',
                                    'TB': 'passes_between_into_open_space',
                                    'Sw': 'passes_traveled_40_yards',
                                    'Crs': 'crosses',
                                    'TI': 'throw_ins_taken',
                                    'CK': 'corner_kicks',
                                    'In': 'inswinging_corner_kicks',
                                    'Out': 'outswinging_corner_kicks',
                                    'Str': 'straight_corner_kicks',
                                    'Cmp': 'passes_completed',
                                    'Off': 'offsides',
                                    'Blocks': 'blocked_by_opponent_in_the_path'},
                           inplace=True)
            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_playingtime.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    'Age': 'age_weighted_by_min_played',
                                    'MP': 'matches_played',
                                    'Min': 'minutes_played',
                                    'Mn/MP': 'minutes_per_match',
                                    'Min%': 'percentage_of_min_played',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Starts': 'games_started_by_player',
                                    'Mn/Start': 'min_per_match_started',
                                    'Compl': 'complete_matches_played',
                                    'Subs': 'games_as_substitute',
                                    'Mn/Sub': 'min_per_substitution',
                                    'unSub': 'games_as_unused_substitute',
                                    'PPM': 'points_per_match',
                                    'onG': 'goals_scored_on_pitch',
                                    'onGA': 'goals_allowed_on_pitch',
                                    '+/-': 'goals_scored_minus_goal_allowed',
                                    '+/-90': 'goals_scored_minus_goal_allowed_per_90_min',
                                    'onxG': 'expected_goals_on_pitch',
                                    'onxGA': 'expected_goals_allowed_on_pitch',
                                    'xG+/-': 'expected_goals_scored_minus_expected_goals_allowed',
                                    'xG+/-90': 'expected_goals_scored_minus_expected_goals_allowed_per_90_min'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_possession.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    'Poss': 'possession',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Touches': 'touches',
                                    'Def Pen': 'touches_in_defensive_penalty_area',
                                    'Def 3rd': 'touches_in_defensive_1_3',
                                    'Mid 3rd': 'touches_in_middle_1_3',
                                    'Att 3rd': 'touches_in_attacking_1_3',
                                    'Att Pen': 'touches_in_attacking_penalty_area',
                                    'Live': 'live_ball_touches',
                                    'Succ': 'dribbles_successfully',
                                    'Att': 'dribbles_attempted',
                                    'Succ%': 'percentage_of_successfully_dribbled',
                                    'Mis': 'player_failed_gain_ball_control',
                                    'Dis': 'player_loses_ball_control_after_tackling',
                                    'Rec': 'passes_received_successfully',
                                    'Prog': 'progressive_passes_received'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_shooting.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Gls': 'goals_scored_or_allowed',
                                    'Sh': 'shots_total',
                                    'SoT': 'shots_on_target',
                                    'SoT%': 'shots_on_target_percent',
                                    'Sh/90': 'shots_total_per_90_min',
                                    'SoT/90': 'shots_on_target_per_90_min',
                                    'G/Sh': 'goals_per_shot',
                                    'G/SoT': 'goals_per_shot_on_target',
                                    'Dist': 'shots_average_distance',
                                    'FK': 'shots_from_free_kicks',
                                    'PK': 'penalty_kicks_made',
                                    'PKatt': 'penalty_kicks_attempted',
                                    'xG': 'expected_goals',
                                    'npxG': 'non_penalty_expected_goals',
                                    'npxG/Sh': 'non_penalty_expected_goals_per_shots',
                                    'G-xG': 'goals_minus_expected_goals',
                                    'np:G-xG': 'non_penalty_goals_minus_expected_goals'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_stats.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'players_used_in_game',
                                    'Age': 'age_weighted_by_min_played',
                                    'Poss': 'possession',
                                    'MP': 'matches_played',
                                    'Starts': 'games_started_by_player',
                                    'Min': 'minutes_played',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'Gls': 'goals_scored_or_allowed',
                                    'Ast': 'assists',
                                    'G-PK': 'non_penalty_goals',
                                    'PK': 'penalty_kicks_made',
                                    'PKatt': 'penalty_kicks_attempted',
                                    'CrdY': 'yellow_cards',
                                    'CrdR': 'red_cards',
                                    'Gls.1': 'goals_scored_per_90_min',
                                    'Ast.1': 'assists_per_90_min',
                                    'G+A': 'goal_and_assists_per_90_min',
                                    'G-PK.1': 'goals_minus_penalty_kicks_per_90_min',
                                    'G+A-PK': 'goals_plus_assists_minus_penalty_kicks_per_90_min',
                                    'xG': 'expected_goals',
                                    'npxG': 'non_penalty_expected_goals',
                                    'xAG': 'expected_assisted_goals',
                                    'npxG+xAG': 'non_penalty_expected_goals_plus_assisted_goals',
                                    'xG.1': 'expected_goals_per_90_min',
                                    'xAG.1': 'expected_assisted_goals_per_90_min',
                                    'xG+xAG': 'expected_goals_plus_assisted_goals_per_90_min',
                                    'npxG.1': 'non_penalty_expected_goals_per_90_min',
                                    'npxG+xAG.1': 'non_penalty_expected_goals_plus_assisted_goals_per_90_min'},
                           inplace=True)

            cols_to_use = content.columns.difference(result.columns)
            result = pd.merge(pd.DataFrame(result),
                              pd.DataFrame(content[cols_to_use]),
                              left_index=True,
                              right_index=True,
                              how='inner')

        if file_name == "squad_data_keepers.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'keepers_used_in_game',
                                    'MP': 'matches_played',
                                    'Starts': 'games_started_by_player',
                                    'Min': 'minutes_played',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'GA': 'keeper_goals_against',
                                    'GA90': 'keeper_goals_against_per_90_min',
                                    'SoTA': 'keeper_shots_on_target_against',
                                    'Saves': 'keeper_saves',
                                    'Save%': 'keeper_save_percentage',
                                    'W': 'keeper_wins',
                                    'D': 'keeper_draws',
                                    'L': 'keeper_losses',
                                    'CS': 'keeper_clean_sheets',
                                    'CS%': 'keeper_clean_sheet_percentage',
                                    'PKatt': 'keeper_penalty_kicks_attempted',
                                    'PKA': 'keeper_penalty_kicks_allowed',
                                    'PKsv': 'keeper_penalty_kicks_saved',
                                    'PKm': 'keeper_penalty_kicks_missed',
                                    'Save%.1': 'keeper_penalty_kicks_percentage'},
                           inplace=True)

            keepers_result = pd.DataFrame(content)

        if file_name == "squad_data_keepersadv.csv":
            content = pd.read_csv(file)
            content.rename(columns={'Squad': 'squad',
                                    '# Pl': 'keepers_used_in_game',
                                    '90s': 'minutes_plays_divided_by_90',
                                    'GA': 'keeper_goals_against',
                                    'PKA': 'keeper_penalty_kicks_allowed',
                                    'FK': 'keeper_free_kicks_goal_against',
                                    'CK': 'keeper_corner_kick_goal_against',
                                    'OG': 'keeper_own_goal_scored_against',
                                    'PSxG': 'keeper_post_shot_expected_goals',
                                    'PSxG/SoT': 'keeper_post_shot_expected_goals_per_shot_on_target',
                                    'PSxG+/-': 'keeper_post_shot_expected_goals_minus_goals_allowed',
                                    '/90': 'keeper_post_shot_expected_goals_minus_goals_allowed_per_90_min',
                                    'Cmp': 'keeper_passes_longer_40_yards',
                                    'Att': 'keeper_passes_attempted_longer_40_yards',
                                    'Cmp%': 'keeper_passes_longer_40_yards_percentage',
                                    'Att.1': 'keeper_passes_attempted',
                                    'Thr': 'keeper_throws_attempted',
                                    'Launch%': 'keeper_launched_passes_percentage',
                                    'AvgLen': 'keeper_passes_avg_length_in_yards',
                                    'Att.2': 'keeper_goals_kicks_attempted',
                                    'Launch%.1': 'keeper_launched_goal_kicks_percentage_longer_40_yards',
                                    'AvgLen.1': 'keeper_goal_kicks_avg_length_in_yards',
                                    'Opp': 'keeper_opponents_attempted_crosses_into_penalty_area',
                                    'Stp': 'keeper_crosses_into_penalty_area_stopped',
                                    'Stp%': 'keeper_crosses_into_penalty_area_stopped_percentage',
                                    '#OPA': 'keeper_defensive_actions_outside_of_penalty_area',
                                    '#OPA/90': 'keeper_defensive_actions_outside_of_penalty_area_percentage',
                                    'AvgDist': 'keeper_avg_distance_from_goal'},
                           inplace=True)

            cols_to_use = content.columns.difference(keepers_result.columns)
            keepers_result = pd.merge(pd.DataFrame(keepers_result),
                                      pd.DataFrame(content[cols_to_use]),
                                      left_index=True,
                                      right_index=True,
                                      how='inner')

    cols_to_use = keepers_result.columns.difference(result.columns)
    result = pd.merge(pd.DataFrame(result),
                      pd.DataFrame(keepers_result[cols_to_use]),
                      left_index=True,
                      right_index=True,
                      how='inner')

    result[['squad_id', 'squad']] = result['squad'].str.split(' ', n=1,
                                                              expand=True)
    result['squad_id'] = result['squad_id'].str.upper()

    ranking_content = pd.read_csv("/opt/airflow/data/fifa_world_ranking.csv",
                                  usecols=["RK", "Unnamed: 1", "Team", "Total Points"])
    ranking_content.rename(columns={'RK': 'ranking',
                                    'Unnamed: 1': 'team',
                                    'Team': 'ranking_total_points',
                                    'Total Points': 'ranking_previous_points'},
                           inplace=True)
    ranking_content.replace("USA", "United States", inplace=True)
    result = pd.merge(pd.DataFrame(result),
                      pd.DataFrame(ranking_content),
                      how='inner',
                      left_on='squad', right_on='team')
    result.drop(['team'], inplace=True, axis=1)
    result = result.fillna(0)

    result.astype({
        'squad': str,
        'players_used_in_game': int,
        'minutes_plays_divided_by_90': float,
        'players_tackled': int,
        'tackles_won': int,
        'tackles_in_defensive_1_3': int,
        'tackles_in_middle_1_3': int,
        'tackles_in_attacking_1_3': int,
        'dribbles_tackles': int,
        'dribbled_past_plus_tackles': int,
        'percentage_of_dribbles_tackles': float,
        'dribbles_pass_by_an_opposing_player': int,
        'blocking_the_ball': int,
        'blocking_a_shot': int,
        'blocking_a_pass': int,
        'interceptions': int,
        'players_tackled_plus_interceptions': int,
        'clearances': int,
        'mistakes_leading_to_an_opponents_shot': int,
        'dead_ball_passes_to_goal': int,
        'dead_ball_passes_to_shot_attempt': int,
        'defensive_action_to_goal': int,
        'defensive_actions_to_shot_attempt': int,
        'dribbles_to_goal': int,
        'dribbles_to_shot_attempt': int,
        'fouls_drawn_to_goal': int,
        'fouls_drawn_to_shot_attempt': int,
        'goal_creating_actions': int,
        'goal_creating_actions_per_90_min': float,
        'live_ball_passes_to_goal': int,
        'pass_live_passes_to_shot_attempt': int,
        'shot_creating_action': int,
        'shot_creating_action_per_90_min': float,
        'shots_to_another_goal_scoring_shot': int,
        'shots_to_another_shot_attempt': int,
        'aerials_lost': int,
        'aerials_won': int,
        'crosses': int,
        'fouls_committed': int,
        'fouls_drawn': int,
        'loss_ball_recover': int,
        'offside': int,
        'own_goals': int,
        'penalty_kicks_conceded': int,
        'penalty_kicks_won': int,
        'percentage_aerials_won': float,
        'red_cards': int,
        'second_yellow_card': int,
        'yellow_cards': int,
        'assisted_shots': int,
        'assists': int,
        'assists_minus_expected_goals_assisted': float,
        'crosses_18_yard_box': int,
        'expected_assisted_goals': float,
        'expected_assists': float,
        'pass_completion_percent': float,
        'pass_completion_percent_15_30_yards': float,
        'pass_completion_percent_5_15_yards': float,
        'pass_completion_percent_longer_30_yards': float,
        'passes_18_yard_box': int,
        'passes_attempted': int,
        'passes_attempted_15_30_yards': int,
        'passes_attempted_5_15_yards': int,
        'passes_attempted_longer_30_yards': int,
        'passes_completed': int,
        'passes_completed_15_30_yards': int,
        'passes_completed_5_15_yards': int,
        'passes_completed_longer_30_yards': int,
        'passes_enter_1_3_pitch_closest_to_goal': int,
        'progressive_distance': int,
        'progressive_passes': int,
        'total_distance_in_yards': int,
        'blocked_by_opponent_in_the_path': int,
        'corner_kicks': int,
        'dead_ball_passes': int,
        'inswinging_corner_kicks': int,
        'live_ball_passes': int,
        'offsides': int,
        'outswinging_corner_kicks': int,
        'passes_attempted_from_free_kicks': int,
        'passes_between_into_open_space': int,
        'passes_traveled_40_yards': int,
        'straight_corner_kicks': int,
        'throw_ins_taken': int,
        'age_weighted_by_min_played': float,
        'complete_matches_played': int,
        'expected_goals_allowed_on_pitch': float,
        'expected_goals_on_pitch': float,
        'expected_goals_scored_minus_expected_goals_allowed': float,
        'expected_goals_scored_minus_expected_goals_allowed_per_90_min': float,
        'games_as_substitute': int,
        'games_as_unused_substitute': int,
        'games_started_by_player': int,
        'goals_allowed_on_pitch': int,
        'goals_scored_minus_goal_allowed_per_90_min': float,
        'goals_scored_minus_goal_allowed': int,
        'goals_scored_on_pitch': int,
        'matches_played': int,
        'min_per_match_started': int,
        'min_per_substitution': int,
        'minutes_per_match': int,
        'minutes_played': int,
        'percentage_of_min_played': int,
        'points_per_match': float,
        'dribbles_attempted': int,
        'dribbles_successfully': int,
        'live_ball_touches': int,
        'passes_received_successfully': int,
        'percentage_of_successfully_dribbled': float,
        'player_failed_gain_ball_control': int,
        'player_loses_ball_control_after_tackling': int,
        'possession': float,
        'progressive_passes_received': int,
        'touches': int,
        'touches_in_attacking_1_3': int,
        'touches_in_attacking_penalty_area': int,
        'touches_in_defensive_1_3': int,
        'touches_in_defensive_penalty_area': int,
        'touches_in_middle_1_3': int,
        'expected_goals': float,
        'goals_minus_expected_goals': float,
        'goals_per_shot': float,
        'goals_per_shot_on_target': float,
        'goals_scored_or_allowed': int,
        'non_penalty_expected_goals': float,
        'non_penalty_expected_goals_per_shots': float,
        'non_penalty_goals_minus_expected_goals': float,
        'penalty_kicks_attempted': int,
        'penalty_kicks_made': int,
        'shots_average_distance': float,
        'shots_from_free_kicks': int,
        'shots_on_target': int,
        'shots_on_target_per_90_min': float,
        'shots_on_target_percent': float,
        'shots_total': int,
        'shots_total_per_90_min': float,
        'assists_per_90_min': float,
        'expected_assisted_goals_per_90_min': float,
        'expected_goals_per_90_min': float,
        'expected_goals_plus_assisted_goals_per_90_min': float,
        'goal_and_assists_per_90_min': float,
        'goals_minus_penalty_kicks_per_90_min': float,
        'goals_plus_assists_minus_penalty_kicks_per_90_min': float,
        'goals_scored_per_90_min': float,
        'non_penalty_expected_goals_per_90_min': float,
        'non_penalty_expected_goals_plus_assisted_goals': float,
        'non_penalty_expected_goals_plus_assisted_goals_per_90_min': float,
        'non_penalty_goals': int,
        'keeper_avg_distance_from_goal': float,
        'keeper_clean_sheet_percentage': float,
        'keeper_clean_sheets': int,
        'keeper_corner_kick_goal_against': int,
        'keeper_crosses_into_penalty_area_stopped': int,
        'keeper_crosses_into_penalty_area_stopped_percentage': float,
        'keeper_defensive_actions_outside_of_penalty_area': int,
        'keeper_defensive_actions_outside_of_penalty_area_percentage': float,
        'keeper_draws': int,
        'keeper_goal_kicks_avg_length_in_yards': float,
        'keeper_goals_against': int,
        'keeper_goals_against_per_90_min': float,
        'keeper_goals_kicks_attempted': int,
        'keeper_launched_goal_kicks_percentage_longer_40_yards': float,
        'keeper_launched_passes_percentage': float,
        'keeper_losses': int,
        'keeper_opponents_attempted_crosses_into_penalty_area': int,
        'keeper_own_goal_scored_against': int,
        'keeper_passes_attempted': int,
        'keeper_passes_attempted_longer_40_yards': int,
        'keeper_passes_avg_length_in_yards': float,
        'keeper_passes_longer_40_yards': int,
        'keeper_passes_longer_40_yards_percentage': float,
        'keeper_penalty_kicks_allowed': int,
        'keeper_penalty_kicks_attempted': int,
        'keeper_penalty_kicks_missed': int,
        'keeper_penalty_kicks_percentage': float,
        'keeper_penalty_kicks_saved': int,
        'keeper_post_shot_expected_goals': float,
        'keeper_post_shot_expected_goals_minus_goals_allowed': float,
        'keeper_post_shot_expected_goals_minus_goals_allowed_per_90_min': float,
        'keeper_post_shot_expected_goals_per_shot_on_target': float,
        'keeper_save_percentage': float,
        'keeper_saves': int,
        'keeper_shots_on_target_against': int,
        'keeper_throws_attempted': int,
        'keeper_wins': int,
        'keeper_free_kicks_goal_against': int,
        'keepers_used_in_game': int,
        'squad_id': str,
        'ranking': int,
        'ranking_total_points': float,
        'ranking_previous_points': float
    })

    with open("/opt/airflow/sql/insert_squad_data_query.sql", "w") as f:
        for index, row in result.iterrows():
            f.write(
                'INSERT INTO "SQUAD" ("' + str(
                    '", "'.join(result.columns)) + '") VALUES ' + str(
                    tuple(row.values)) + ';\n'
            )

    with open("/opt/airflow/sql/create_table_squad_data_query.sql", "w") as f:
        drop_table = 'DROP TABLE IF EXISTS "SQUAD";' + '\n'
        create_table = pd.io.sql.get_schema(result, 'SQUAD')
        f.write(
            drop_table + create_table
        )


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    print(_squad_data_transformation())
