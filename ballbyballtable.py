import os
from json_helper import load_json
from datetime import datetime
import dateutil.parser as parser
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
from json_helper import load_json, dump_file
import requests


class BallbyBall:
    def __init__(self, directory_json_ball_by_ball, tabular_directory, json_directory):
        self.directory = directory_json_ball_by_ball
        self.tabular_directory = tabular_directory
        self.files = os.listdir(self.directory)
        self.json_directory = json_directory
        self.files = os.listdir(self.directory)

    @staticmethod
    def get_match_details(data):
        date_string = data['info']['dates'][0]
        year = parser.parse(date_string).year
        venue = data['info']['venue']
        venue = venue.split(",")[0]
        if "match_number" in data['info']['event'].keys():
            match_id = str(year) + "_" + str(data['info']['event']['match_number'])
        else:
            match_id = str(year) + "_" + str(data['info']['event']["stage"])
        return match_id, venue

    @staticmethod
    def batting_first_second(data):
        toss = data['info']['toss']
        outcome = data['info']['outcome']
        decision = toss['decision']
        winner = toss['winner']
        other = list(data['info']['teams'])
        other.remove(winner)
        other = other[0]
        who_won = None
        who_lost = None
        if decision == "field":
            bat1 = other
            bat2 = winner
        else:
            bat1 = winner
            bat2 = other
        if "winner" in outcome.keys():
            if outcome['winner'] in [bat2, bat1]:
                winner = outcome['winner']
                margin = outcome['by']
                if winner == bat1:
                    who_won = "bat1"
                    who_lost = "bat2"
                else:
                    who_won = "bat2"
                    who_lost = "bat1"
        else:
            winner = ""
            who_won = None
            who_lost = None

        return bat1, bat2, winner, who_won, who_lost

    @staticmethod
    def get_ball_by_ball_stats(over_no, ball_no, ball_json, match_id, innings, id_dict, style_dict):
        batter = ball_json['batter']
        batter_id = id_dict[batter]
        batting_style = style_dict[batter_id]['batting_style']
        bowler = ball_json['bowler']
        bowler_id = id_dict[bowler]
        bowler_syle = style_dict[bowler_id]['bowling_style']
        non_striker = ball_json['non_striker']
        b_run = ball_json['runs']['batter']
        t_run = ball_json['runs']['total']
        e_run = ball_json['runs']['extras']
        wickets = 0
        player_out = ""
        kind = ""
        extras = ""
        if batting_style == "":
            batting_style = "not know"
        if bowler_syle == "":
            bowler_syle = " no known"
        if "wickets" in ball_json.keys():
            wicket = ball_json['wickets'][0]
            wickets = 1
            player_out = wicket["player_out"]
            kind = wicket["kind"]
        if "extras" in ball_json.keys():
            extras = list(ball_json['extras'].keys())[0]
        print(batting_style)

        return match_id, innings, over_no, ball_no, batter, bowler, non_striker, b_run, t_run, e_run, wickets, player_out, kind, extras, batting_style, bowler_syle

    def get_ball_by_ball(self, file_name, id_dict, style_dict):
        data = load_json(file_name=file_name, directory=self.directory)
        match_id, venue = self.get_match_details(data)
        overs = data['innings'][0]['overs']

        values = [
            self.get_ball_by_ball_stats(over['over'], delivery_index, over['deliveries'][delivery_index], match_id, 1,
                                        id_dict, style_dict)
            for
            over in overs
            for delivery_index in
            range(len(over['deliveries']))]
        inn_1 = pd.DataFrame(
            columns=['match_id', "innings", "over", "ball", "batter", "bowler", "non_striker", "batter_run",
                     "total_run",
                     "extra_run", "wickets", "player_out", "kind", "extras", "batter_style", "bowler_style"],
            data=values)
        values = [
            self.get_ball_by_ball_stats(over['over'], delivery_index, over['deliveries'][delivery_index], match_id, 2,
                                        id_dict, style_dict)
            for
            over in overs
            for delivery_index in
            range(len(over['deliveries']))]
        inn_2 = pd.DataFrame(
            columns=['match_id', "innings", "over", "ball", "batter", "bowler", "non_striker", "batter_run",
                     "total_run",
                     "extra_run", "wickets", "player_out", "kind", "extras", "batter_style", "bowler_style"],
            data=values)
        match = pd.concat([inn_1, inn_2])
        return match

    def get_player_id(self, file_name):
        data = load_json(file_name=file_name, directory=self.directory)
        players_id = {}
        players_dict = data['info']['players']
        t1, t2 = players_dict.keys()
        people = data['info']["registry"]['people']
        players = players_dict[t1]
        players.extend(players_dict[t2])
        for player in players:
            players_id[people[player]] = player
        return players_id

    def get_player_id_dict(self):
        files = self.files

        dicts = [self.get_player_id(file_name=file) for file in tqdm(files) if
                 file.endswith(".json")]
        super_dict = {val: key for d in dicts for key, val in d.items()}
        return super_dict

    def match_information(self, file_name):
        data = load_json(file_name=file_name, directory=self.directory)
        match_id, venue = self.get_match_details(data)
        bat1, bat2, winner, who_won, who_lost = self.batting_first_second(data)
        return match_id, venue, bat1, bat2, winner, who_won, who_lost

    def get_match_info_all_matches(self):
        files = os.listdir(self.directory)
        values = [self.match_information(file) for file in tqdm(files) if file.endswith(".json")]
        mathc_df = pd.DataFrame(columns=['match_id', "venue", "bat1", "bat2", "winner", "who_won", "who_lost"],
                                data=values)
        mathc_df.to_parquet(os.path.join(self.tabular_directory, "match.parquet"))

    def agg_ball_by_ball(self):
        files = os.listdir(self.directory)
        id_dict = self.get_player_id_dict()
        style_dict = load_json(file_name="playing_styles.json", directory="json_data")
        dfs = [self.get_ball_by_ball(file_name=file, id_dict=id_dict, style_dict=style_dict) for file in tqdm(files) if
               file.endswith(".json")]
        all_dfs = pd.concat(dfs)
        all_dfs.to_parquet(os.path.join(self.tabular_directory, "all_ball_by_ball.parquet"))


bbb = BallbyBall(directory_json_ball_by_ball="ipl_ball_by_ball", tabular_directory="tabular_data",
                 json_directory="json_data")
# bbb.get_match_info_all_matches()
bbb.agg_ball_by_ball()
