import os

from json_helper import load_json
from datetime import datetime
import dateutil.parser as parser
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
from json_helper import load_json, dump_file
import requests


class MatchData:
    def __init__(self, directory_json_ball_by_ball, tabular_directory, json_directory):
        self.data = ""
        self.directory = directory_json_ball_by_ball
        self.tabular_directory = tabular_directory
        self.files = os.listdir(self.directory)
        self.json_directory = json_directory

    def get_match_information(self, file_name, mode):
        self.data = load_json(file_name=file_name, directory=self.directory)
        date_string = self.data['info']['dates'][0]
        year = parser.parse(date_string).year
        venue = self.data['info']['venue']
        people = self.data['info']["registry"]['people']

        if "match_number" in self.data['info']['event'].keys():
            match_id = str(year) + "_" + str(self.data['info']['event']['match_number'])
        else:
            match_id = str(year) + "_" + str(self.data['info']['event']["stage"])

        bat1, bat2, winner, who_won = self.batting_first_second(self.data['info']['toss'], self.data['info']['outcome'])
        if 'player_of_match' in self.data['info'].keys():
            mom = self.data['info']['player_of_match'][0]
        else:
            mom = ""
        player_stats_t1 = {
            i: {"runs": 0, "wickets": 0, "fours": 0, "sixes": 0, "team": bat1, "id": people[i], "out": "dnb",
                "balls_faced": 0, "runs_given": 0, "deliveries_bowled": 0, "wicket_bowler": "", "sixes_conceded": 0,
                "four_conceded": 0, "year": year, "innings": "bat1", "who_won": who_won, "opposition": bat2,
                "batter_dot": 0, "bowler_dot": 0} for
            i in
            self.data['info']['players'][bat1]}
        player_stats_t2 = {
            i: {"runs": 0, "wickets": 0, "fours": 0, "sixes": 0, "team": bat2, "id": people[i], "out": "dnb",
                "balls_faced": 0, "runs_given": 0, "deliveries_bowled": 0, "wicket_bowler": "", "sixes_conceded": 0,
                "four_conceded": 0, "year": year, "innings": "bat2", "who_won": who_won, "opposition": bat1,
                "batter_dot": 0, "bowler_dot": 0} for
            i in
            self.data['info']['players'][bat2]}
        extras = {"wides": 0, "noballs": 0, "byes": 0, "legbyes": 0, "penalty": 0}
        out = {"lbw": 0, "bowled": 0, "caught": 0, "run out": 0, 'stumped': 0, 'caught and bowled': 0, 'hit wicket': 0,
               'retired hurt': 0, 'obstructing the field': 0}
        self.innings_score_card(overs=self.data['innings'][0]['overs'], player_stats=player_stats_t1,
                                player_stats_2=player_stats_t2, extras=extras, out=out)
        out_innings1 = out.copy()
        extras_innings1 = extras.copy()
        if len(self.data['innings']) == 2:
            self.innings_score_card(overs=self.data['innings'][1]['overs'], player_stats=player_stats_t2,
                                    player_stats_2=player_stats_t1, extras=extras, out=out)
        extras_innings2 = {key: (extras[key] - extras_innings1[key]) for key in extras.keys()}

        out_innings2 = {key: out[key] - out_innings1[key] for key in out.keys()}
        if mom != "":
            if mom in player_stats_t1.keys():
                mom_performance = player_stats_t1[mom]
            else:
                mom_performance = player_stats_t2[mom]
        else:
            mom_performance = {}
        score_card_1 = pd.DataFrame.from_dict(player_stats_t1, orient='index')
        score_card_2 = pd.DataFrame.from_dict(player_stats_t2, orient='index')
        fi_score = score_card_1['runs'].sum() + sum(list(extras_innings1.values()))
        se_score = score_card_2['runs'].sum() + sum(list(extras_innings2.values()))
        fi_six = score_card_1['sixes'].sum()
        si_six = score_card_2['sixes'].sum()
        fi_four = score_card_1['fours'].sum()
        si_four = score_card_2['fours'].sum()
        output = {"mathc_id": match_id,
                  "year": year,
                  "bat1": bat1,
                  "bat2": bat2,
                  "winner": winner,
                  "who_won": who_won,
                  "mom": mom,
                  "mom_performance": mom_performance,
                  "score_inn1": fi_score,
                  "score_inn2": se_score,
                  "fi_four": fi_four,
                  "fi_six": fi_six,
                  "se_four": si_four,
                  "se_six": si_six,
                  "venue": venue

                  }
        for keys in extras_innings1.keys():
            output[keys + "_inn_1"] = extras_innings1[keys]
            output[keys + "_inn_2"] = extras_innings2[keys]
        for keys in out.keys():
            output[keys + "_inn_1"] = out_innings1[keys]
            output[keys + "_inn_2"] = out_innings2[keys]
        if mode == "dict":
            return output
        elif mode == "df":
            stats_df = pd.concat([score_card_1, score_card_2])
            stats_df = stats_df.reset_index().rename(columns={'index': "name"})
            return stats_df

    def batting_first_second(self, toss, outcome):
        decision = toss['decision']
        winner = toss['winner']
        other = list(self.data['info']['teams'])
        other.remove(winner)
        other = other[0]

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
                else:
                    who_won = "bat2"
        else:
            winner = ""
            who_won = None

        return bat1, bat2, winner, who_won

    @staticmethod
    def ball_aggregator(deliveries, player_stats, player_stats_2, extras, out):
        batter = deliveries['batter']
        bowler = deliveries['bowler']

        player_stats[batter]['out'] = "not out"
        if "extras" in deliveries:
            if "no ball" in deliveries['extras'].keys() or "wides" in deliveries['extras'].keys():
                pass
            else:
                player_stats_2[bowler]["deliveries_bowled"] = player_stats_2[bowler]["deliveries_bowled"] + 1

        else:
            player_stats_2[bowler]["deliveries_bowled"] = player_stats_2[bowler]["deliveries_bowled"] + 1

        if "extras" in deliveries:
            if "wides" in deliveries['extras'].keys():
                pass
            else:
                player_stats[batter]['balls_faced'] = player_stats[batter]['balls_faced'] + 1
        else:
            player_stats[batter]['balls_faced'] = player_stats[batter]['balls_faced'] + 1

        runs = deliveries['runs']['batter']
        runs_given = deliveries['runs']['total']
        if runs == 0:
            player_stats[batter]['batter_dot'] = player_stats[batter]["batter_dot"] + 1
        if runs_given == 0:
            player_stats_2[bowler]['bowler_dot'] = player_stats_2[bowler]["bowler_dot"] + 1

        player_stats_2[bowler]['runs_given'] = player_stats_2[bowler]['runs_given'] + runs_given
        player_stats[batter]['runs'] = player_stats[batter]['runs'] + runs
        if runs == 4:
            player_stats[batter]['fours'] = player_stats[batter]['fours'] + 1
            player_stats_2[bowler]["four_conceded"] = player_stats_2[bowler]["four_conceded"] + 1
        elif runs == 6:
            player_stats[batter]['sixes'] = player_stats[batter]['sixes'] + 1
            player_stats_2[bowler]["sixes_conceded"] = player_stats_2[bowler]["sixes_conceded"] + 1

        if "wickets" in deliveries.keys():
            wicket = deliveries['wickets'][0]
            player_out = wicket['player_out']
            player_stats[player_out]['out'] = "out"
            kind = wicket['kind']
            if kind in out.keys():
                out[kind] = out[kind] + 1
            else:
                out[kind] = 1
            if wicket['kind'] != "run out":
                bowler = deliveries['bowler']
                player_stats[batter]['wicket_bowler'] = bowler
                player_stats_2[bowler]['wickets'] = player_stats_2[bowler]['wickets'] + 1
        if "extras" in deliveries.keys():
            extra = deliveries['extras']
            key = list(extra.keys())[0]
            value = list(extra.values())[0]
            extras[key] = extras[key] + value

    def innings_score_card(self, overs, player_stats, player_stats_2, extras, out):
        _ = [self.ball_aggregator(delivery, player_stats, player_stats_2, extras, out) for over in overs
             for delivery in
             over['deliveries']]

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


class AggregateData(MatchData):
    def get_matches_data_overall(self, file_name):
        files = self.files
        files = os.listdir(self.directory)
        matches_dict = [self.get_match_information(file_name=file, mode="dict") for file in tqdm(files) if
                        file.endswith(".json")]

        matches_record_df = pd.DataFrame(matches_dict)
        matches_record_df[['venue', 'y']] = matches_record_df['venue'].str.split(',', 1, expand=True)
        del matches_record_df['y']

        matches_record_df.to_parquet(os.path.join(self.tabular_directory, file_name))
        return matches_record_df

    @staticmethod
    def calulate_overs(balls):
        if balls % 6 == 0:
            return balls / 6
        else:
            return balls // 6 + (balls % 6) * 0.1

    @staticmethod
    def hundereds(runs):
        if runs > 100:
            return 1
        else:
            return 0

    @staticmethod
    def fifty(runs):
        if runs in range(50, 100):
            return 1
        else:
            return 0

    @staticmethod
    def bowling_milestone(wickets):
        if wickets >= 4:
            return 1
        else:
            return 0

    def get_player_data(self, file_name):
        # dicts = [self.get_player_id(file_name=file) for file in tqdm(self.files) if file.endswith(".json")]
        # super_dict = {key: val for d in dicts for key, val in d.items()}
        dfs = [self.get_match_information(file_name=file, mode="df") for file in tqdm(self.files) if
               file.endswith(".json")]
        mega_df = pd.concat(dfs)
        mega_df['overs'] = mega_df["deliveries_bowled"].apply(self.calulate_overs)
        mega_df['hund'] = mega_df["runs"].apply(self.hundereds)
        mega_df['fifty'] = mega_df["runs"].apply(self.fifty)

        mega_df['four_wicket'] = mega_df["wickets"].apply(self.bowling_milestone)
        mega_df.to_parquet(os.path.join(self.tabular_directory, file_name))
        return mega_df

    def get_player_id_dict(self, ids_map_csv):
        dicts = [self.get_player_id(file_name=file) for file in tqdm(self.files) if file.endswith(".json")]
        super_dict = {key: val for d in dicts for key, val in d.items()}
        # dump_file("player_ids", "json_data", super_dict)
        df = pd.read_csv(os.path.join(self.directory, ids_map_csv))

        out_dict = {}
        for ids in tqdm(super_dict.keys()):
            temp = df[df['identifier'] == ids]
            cric_info_id = int(temp['key_cricinfo'])
            try:
                json_url = "http://core.espnuk.org/v2/sports/cricket/athletes/{0}".format(cric_info_id)
                response = requests.get(json_url)
                if response.status_code == 200:
                    styles = response.json()['styles']
                    if len(styles) == 2:
                        batting = styles[0]['description']
                        bowling = styles[1]['description']
                    elif len(styles) == 1:
                        batting = styles[0]['description']
                        bowling = ""
                    else:
                        batting = ""
                        bowling = ""
                    out_dict[ids] = {"batting_style": batting, "bowling_style": bowling}
            except:
                out_dict[ids] = {"batting_style": "", "bowling_style": ""}
        dump_file("playing_styles.json", directory="json_data", dict_=out_dict)


ad = AggregateData(directory_json_ball_by_ball="ipl_ball_by_ball", tabular_directory="tabular_data",
                   json_directory="json_data")
# ad.get_player_id_dict(ids_map_csv="people.csv")
# ad.get_matches_data_overall(file_name="matches_data_agg.parquet")
ad.get_player_data(file_name="players_stat.parquet")
