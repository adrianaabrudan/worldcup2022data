DROP TABLE IF EXISTS "SQUAD";
CREATE TABLE "SQUAD" (
"squad" TEXT,
  "players_used_in_game" INTEGER,
  "minutes_plays_divided_by_90" REAL,
  "players_tackled" INTEGER,
  "tackles_won" INTEGER,
  "tackles_in_defensive_1_3" INTEGER,
  "tackles_in_middle_1_3" INTEGER,
  "tackles_in_attacking_1_3" INTEGER,
  "dribbles_tackles" INTEGER,
  "dribbled_past_plus_tackles" INTEGER,
  "percentage_of_dribbles_tackles" REAL,
  "dribbles_pass_by_an_opposing_player" INTEGER,
  "blocking_the_ball" INTEGER,
  "blocking_a_shot" INTEGER,
  "blocking_a_pass" INTEGER,
  "interceptions" INTEGER,
  "players_tackled_plus_interceptions" INTEGER,
  "clearances" INTEGER,
  "mistakes_leading_to_an_opponents_shot" INTEGER,
  "dead_ball_passes_to_goal" INTEGER,
  "dead_ball_passes_to_shot_attempt" INTEGER,
  "defensive_action_to_goal" INTEGER,
  "defensive_actions_to_shot_attempt" INTEGER,
  "dribbles_to_goal" INTEGER,
  "dribbles_to_shot_attempt" INTEGER,
  "fouls_drawn_to_goal" INTEGER,
  "fouls_drawn_to_shot_attempt" INTEGER,
  "goal_creating_actions" INTEGER,
  "goal_creating_actions_per_90_min" REAL,
  "live_ball_passes_to_goal" INTEGER,
  "pass_live_passes_to_shot_attempt" INTEGER,
  "shot_creating_action" INTEGER,
  "shot_creating_action_per_90_min" REAL,
  "shots_to_another_goal_scoring_shot" INTEGER,
  "shots_to_another_shot_attempt" INTEGER,
  "aerials_lost" INTEGER,
  "aerials_won" INTEGER,
  "crosses" INTEGER,
  "fouls_committed" INTEGER,
  "fouls_drawn" INTEGER,
  "loss_ball_recover" INTEGER,
  "offside" INTEGER,
  "own_goals" INTEGER,
  "penalty_kicks_conceded" INTEGER,
  "penalty_kicks_won" INTEGER,
  "percentage_aerials_won" REAL,
  "red_cards" INTEGER,
  "second_yellow_card" INTEGER,
  "yellow_cards" INTEGER,
  "assisted_shots" INTEGER,
  "assists" INTEGER,
  "assists_minus_expected_goals_assisted" REAL,
  "crosses_18_yard_box" INTEGER,
  "expected_assisted_goals" REAL,
  "expected_assists" REAL,
  "pass_completion_percent" REAL,
  "pass_completion_percent_15_30_yards" REAL,
  "pass_completion_percent_5_15_yards" REAL,
  "pass_completion_percent_longer_30_yards" REAL,
  "passes_18_yard_box" INTEGER,
  "passes_attempted" INTEGER,
  "passes_attempted_15_30_yards" INTEGER,
  "passes_attempted_5_15_yards" INTEGER,
  "passes_attempted_longer_30_yards" INTEGER,
  "passes_completed" INTEGER,
  "passes_completed_15_30_yards" INTEGER,
  "passes_completed_5_15_yards" INTEGER,
  "passes_completed_longer_30_yards" INTEGER,
  "passes_enter_1_3_pitch_closest_to_goal" INTEGER,
  "progressive_distance" INTEGER,
  "progressive_passes" INTEGER,
  "total_distance_in_yards" INTEGER,
  "blocked_by_opponent_in_the_path" INTEGER,
  "corner_kicks" INTEGER,
  "dead_ball_passes" INTEGER,
  "inswinging_corner_kicks" INTEGER,
  "live_ball_passes" INTEGER,
  "offsides" INTEGER,
  "outswinging_corner_kicks" INTEGER,
  "passes_attempted_from_free_kicks" INTEGER,
  "passes_between_into_open_space" INTEGER,
  "passes_traveled_40_yards" INTEGER,
  "straight_corner_kicks" INTEGER,
  "throw_ins_taken" INTEGER,
  "age_weighted_by_min_played" REAL,
  "complete_matches_played" INTEGER,
  "expected_goals_allowed_on_pitch" REAL,
  "expected_goals_on_pitch" REAL,
  "expected_goals_scored_minus_expected_goals_allowed" REAL,
  "expected_goals_scored_minus_expected_goals_allowed_per_90_min" REAL,
  "games_as_substitute" INTEGER,
  "games_as_unused_substitute" INTEGER,
  "games_started_by_player" INTEGER,
  "goals_allowed_on_pitch" INTEGER,
  "goals_scored_minus_goal_allowed" INTEGER,
  "goals_scored_minus_goal_allowed_per_90_min" REAL,
  "goals_scored_on_pitch" INTEGER,
  "matches_played" INTEGER,
  "min_per_match_started" INTEGER,
  "min_per_substitution" INTEGER,
  "minutes_per_match" INTEGER,
  "minutes_played" INTEGER,
  "percentage_of_min_played" INTEGER,
  "points_per_match" REAL,
  "dribbles_attempted" INTEGER,
  "dribbles_successfully" INTEGER,
  "live_ball_touches" INTEGER,
  "passes_received_successfully" INTEGER,
  "percentage_of_successfully_dribbled" REAL,
  "player_failed_gain_ball_control" INTEGER,
  "player_loses_ball_control_after_tackling" INTEGER,
  "possession" REAL,
  "progressive_passes_received" INTEGER,
  "touches" INTEGER,
  "touches_in_attacking_1_3" INTEGER,
  "touches_in_attacking_penalty_area" INTEGER,
  "touches_in_defensive_1_3" INTEGER,
  "touches_in_defensive_penalty_area" INTEGER,
  "touches_in_middle_1_3" INTEGER,
  "expected_goals" REAL,
  "goals_minus_expected_goals" REAL,
  "goals_per_shot" REAL,
  "goals_per_shot_on_target" REAL,
  "goals_scored_or_allowed" INTEGER,
  "non_penalty_expected_goals" REAL,
  "non_penalty_expected_goals_per_shots" REAL,
  "non_penalty_goals_minus_expected_goals" REAL,
  "penalty_kicks_attempted" INTEGER,
  "penalty_kicks_made" INTEGER,
  "shots_average_distance" REAL,
  "shots_from_free_kicks" INTEGER,
  "shots_on_target" INTEGER,
  "shots_on_target_per_90_min" REAL,
  "shots_on_target_percent" REAL,
  "shots_total" INTEGER,
  "shots_total_per_90_min" REAL,
  "assists_per_90_min" REAL,
  "expected_assisted_goals_per_90_min" REAL,
  "expected_goals_per_90_min" REAL,
  "expected_goals_plus_assisted_goals_per_90_min" REAL,
  "goal_and_assists_per_90_min" REAL,
  "goals_minus_penalty_kicks_per_90_min" REAL,
  "goals_plus_assists_minus_penalty_kicks_per_90_min" REAL,
  "goals_scored_per_90_min" REAL,
  "non_penalty_expected_goals_per_90_min" REAL,
  "non_penalty_expected_goals_plus_assisted_goals" REAL,
  "non_penalty_expected_goals_plus_assisted_goals_per_90_min" REAL,
  "non_penalty_goals" INTEGER,
  "keeper_avg_distance_from_goal" REAL,
  "keeper_clean_sheet_percentage" REAL,
  "keeper_clean_sheets" INTEGER,
  "keeper_corner_kick_goal_against" INTEGER,
  "keeper_crosses_into_penalty_area_stopped" INTEGER,
  "keeper_crosses_into_penalty_area_stopped_percentage" REAL,
  "keeper_defensive_actions_outside_of_penalty_area" INTEGER,
  "keeper_defensive_actions_outside_of_penalty_area_percentage" REAL,
  "keeper_draws" INTEGER,
  "keeper_free_kicks_goal_against" INTEGER,
  "keeper_goal_kicks_avg_length_in_yards" REAL,
  "keeper_goals_against" INTEGER,
  "keeper_goals_against_per_90_min" REAL,
  "keeper_goals_kicks_attempted" INTEGER,
  "keeper_launched_goal_kicks_percentage_longer_40_yards" REAL,
  "keeper_launched_passes_percentage" REAL,
  "keeper_losses" INTEGER,
  "keeper_opponents_attempted_crosses_into_penalty_area" INTEGER,
  "keeper_own_goal_scored_against" INTEGER,
  "keeper_passes_attempted" INTEGER,
  "keeper_passes_attempted_longer_40_yards" INTEGER,
  "keeper_passes_avg_length_in_yards" REAL,
  "keeper_passes_longer_40_yards" INTEGER,
  "keeper_passes_longer_40_yards_percentage" REAL,
  "keeper_penalty_kicks_allowed" INTEGER,
  "keeper_penalty_kicks_attempted" INTEGER,
  "keeper_penalty_kicks_missed" INTEGER,
  "keeper_penalty_kicks_percentage" REAL,
  "keeper_penalty_kicks_saved" INTEGER,
  "keeper_post_shot_expected_goals" REAL,
  "keeper_post_shot_expected_goals_minus_goals_allowed" REAL,
  "keeper_post_shot_expected_goals_minus_goals_allowed_per_90_min" REAL,
  "keeper_post_shot_expected_goals_per_shot_on_target" REAL,
  "keeper_save_percentage" REAL,
  "keeper_saves" INTEGER,
  "keeper_shots_on_target_against" INTEGER,
  "keeper_throws_attempted" INTEGER,
  "keeper_wins" INTEGER,
  "keepers_used_in_game" INTEGER,
  "squad_id" TEXT,
  "ranking" INTEGER,
  "ranking_total_points" REAL,
  "ranking_previous_points" REAL
)