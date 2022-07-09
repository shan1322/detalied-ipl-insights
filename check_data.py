import json
import pandas as pd

f = open("ipl_ball_by_ball/1082616.json")
data = json.load(f)
total = 0
w = 0
c = 0
players = data['info']['players']
t1, t2 = players.keys()
player_stats_t1 = {i: {"runs": 0, "wickets": 0, "fours": 0, "sixes": 0, "team": t1, "wickets": 0} for i in players[t1]}
player_stats_t2 = {i: {"runs": 0, "wickets": 0, "team": t2, "fours": 0, "sixes": 0, "wickets": 0} for i in players[t2]}


def ball_aggregator(deliveries, player_stats, player_stats_2):
    batter = deliveries['batter']
    runs = deliveries['runs']['batter']
    player_stats[batter]['runs'] = player_stats[batter]['runs'] + runs
    if runs == 4:
        player_stats[batter]['fours'] = player_stats[batter]['fours'] + 1
    elif runs == 6:
        player_stats[batter]['sixes'] = player_stats[batter]['fours'] + 1
    if "wickets" in deliveries.keys():
        wicket = deliveries['wickets'][0]
        if wicket['kind'] != "run out":
            bowler = deliveries['bowler']
            player_stats_2[bowler]['wickets'] = player_stats_2[bowler]['wickets'] + 1


def innings_score_card(overs, player_stats, player_stats_2):
    _ = [ball_aggregator(delivery, player_stats, player_stats_2) for over in overs for delivery in over['deliveries']]


innings_score_card(data['innings'][1]['overs'], player_stats=player_stats_t1, player_stats_2=player_stats_t2)
innings_score_card(data['innings'][0]['overs'], player_stats_t2, player_stats_2=player_stats_t1)
df = pd.DataFrame.from_dict(player_stats_t1, orient='index')
df2 = pd.DataFrame.from_dict(player_stats_t2, orient='index')

print(df)
print(df2)
