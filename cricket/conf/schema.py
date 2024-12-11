import polars as pl

class Schemas:
    registry = {
        'match_id': pl.String,
        'person_name': pl.String,
        'person_id': pl.String
    }

    match_info = {
        'match_id': pl.String,
        'city': pl.String,
        'match_start_date': pl.Date,
        'match_end_date': pl.Date,
        'match_type': pl.String,
        'gender': pl.String,
        'umpire_1': pl.String,
        'umpire_2': pl.String,
        'win_by': pl.String,
        'win_margin': pl.Float64,
        'winner': pl.String,
        'player_of_match': pl.String,
        'team1': pl.String,
        'team2': pl.String,
        'toss_decision': pl.String,
        'toss_winner': pl.String,
        'venue': pl.String
    }

    deliveries = {
        'match_id': pl.String,
        'innings': pl.String,
        'batting_team': pl.String,
        'bowling_team': pl.String,
        'declared': pl.Int64,
        'delivery': pl.Float64,
        'batter': pl.String,
        'bowler': pl.String,
        'non_striker': pl.String,
        'batter_runs': pl.Int64,
        'extra_runs': pl.Int64,
        'total_runs': pl.Int64,
        'wicket_type': pl.String,
        'player_out': pl.String
    }