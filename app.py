import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from graph_helper import split_bar_plot, simple_bar_plot, simple_line
import os


def get_overall_tally(dataframe):
    st.title('Overall Tally ')
    agg_data = dataframe.groupby("winner").agg({"winner": "count"}).rename(columns={"winner": "matches_won"})
    agg_data = agg_data.reset_index()
    agg_data.loc[agg_data['winner'] == '', 'winner'] = "no result"
    agg_data = agg_data.sort_values(by=['matches_won'], ascending=False)
    agg_data = agg_data.reset_index(drop=True)
    agg_data.rename({"winner": "teams"}, axis=1, inplace=True)
    agg_data['title'] = [5, 4, 2, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0]
    agg_data = agg_data.sort_values(by=['title', 'matches_won'], ascending=False)
    agg_data = agg_data.reset_index(drop=True)

    st.table(agg_data)


def get_year_wise_data(df):
    agg_data = df.groupby("year").agg({"score_inn1": "mean", "score_inn2": "mean"}).round(0).reset_index()
    fig = simple_bar_plot(data=agg_data, x='year', y='score_inn1', text="score_inn1",
                          color_column="year")
    st.header("Scores batting First")
    st.plotly_chart(fig, use_container_width=False)
    fig = simple_bar_plot(data=agg_data, x='year', y='score_inn2', text="score_inn2", color_column="year")

    st.header("Scores batting Second")
    st.plotly_chart(fig, use_container_width=False)
    bat1_df = df[df['who_won'] == 'bat1'].groupby("year").agg({'who_won': "count"}).rename(
        columns={"who_won": "bat_first"}).reset_index()
    bat2_df = df[df['who_won'] == 'bat2'].groupby("year").agg({'who_won': "count"}).rename(
        columns={"who_won": "bat_second"}).reset_index()
    bat_df = pd.merge(bat1_df, bat2_df, left_on='year', right_on='year')
    fig = split_bar_plot(title_1='matches won batting second', title_2='matches won batting first', x=bat_df["year"],
                         y1=bat_df["bat_first"], y2=bat_df["bat_second"])
    st.header("matches won batting first and matches one batting second year wise")
    st.plotly_chart(fig, use_container_width=False)


def get_stadium_stats(df, stadium):
    df = df[df['venue'] == stadium]
    agg_data = df.groupby(["year"]).agg({"score_inn1": "mean", "score_inn2": "mean"}).reset_index()
    fig = split_bar_plot(title_1="avg innings1 score ", title_2="avg innings2 score ", x=agg_data['year'],
                         y1=agg_data['score_inn1'], y2=agg_data['score_inn2'])
    st.header("stadium")
    st.plotly_chart(fig, use_container_width=False)


def get_boundary_data(df):
    df['total_sixes'] = df['fi_six'] + df['se_six']
    df['total_fours'] = df['fi_four'] + df['se_four']
    agg_data = df.groupby('year').agg({'total_sixes': "sum", "total_fours": "sum"}).reset_index()
    fig = simple_line(data=agg_data, x="year", y="total_sixes", title="sixes")
    st.header("sixes")
    st.plotly_chart(fig, use_container_width=False)
    fig = simple_line(data=agg_data, x="year", y="total_fours", title="fours")
    st.header("fours")
    st.plotly_chart(fig, use_container_width=False)


def get_player_stats(df, min_matches=0, min_strike_rate=0, min_average=0, max_eco=0):
    global batting_df, bowling_df
    if max_eco == 0:
        if os.path.exists("tabular_data/agg_batting.parquet"):
            agg_df = pd.read_parquet("tabular_data/agg_batting.parquet")
        else:
            df['not_out'] = [0 for i in range(len(df))]
            df['dnb'] = [0 for i in range(len(df))]
            df['out_bool'] = [0 for i in range(len(df))]
            df.loc[df['out'] == 'out', 'out_bool'] = 1
            df.loc[df['out'] == 'dnb', 'dnb'] = 1
            df.loc[df['out'] == 'not out', 'not_out'] = 1
            agg_df = df.groupby('name').agg(
                {"runs": "sum", "sixes": "sum", "fours": "sum", "balls_faced": "sum", "hund": "sum",
                 "fifty": "sum", "team": "count", "out_bool": "sum", "dnb": "sum", "not_out": "sum"}).rename(
                columns={"team": "matches"}).round(2).reset_index()
            agg_df['strike_rate'] = (agg_df['runs'] / agg_df['balls_faced']) * 100
            agg_df['average'] = (agg_df['runs'] / agg_df['out_bool'])
            del agg_df['out_bool']
            agg_df = agg_df.sort_values(by=['runs', 'strike_rate'], ascending=False)
            agg_df.to_parquet("tabular_data/agg_batting.parquet")
        agg_df = agg_df[(agg_df['matches'] >= min_matches) & (agg_df['strike_rate'] >= min_strike_rate) & (
                agg_df['average'] >= min_average)]
        batting_df = agg_df
        st.table(agg_df)
    else:

        agg_df = df.groupby('name').agg(
            {"wickets": "sum", "team": "count", "runs_given": "sum", "overs": "sum"}).rename(
            columns={"team": "matches"}).round(0).reset_index()
        agg_df['economy'] = agg_df['runs_given'] / agg_df['overs']
        agg_df = agg_df.sort_values(by=['wickets', 'economy'], ascending=False)
        agg_df.to_parquet("tabular_data/agg_bowling.parquet")
        agg_df['economy'] = agg_df['runs_given'] / agg_df['overs']
        if os.path.isfile("tabular_data/agg_bowling.parquet"):
            pass
        else:
            agg_df.to_parquet("tabular_data/agg_bowling.parquet")

        agg_df = agg_df[(agg_df['matches'] >= min_matches) & (agg_df['economy'] <= max_eco)]
        bowling_df = agg_df
        st.write(agg_df)


def get_hundred_insights(df, strike_rate, mile_stone):
    df = df[df[mile_stone] == 1]
    df.loc[df['innings'] == df['who_won'], 'match_won'] = 1
    df.loc[df['innings'] != df['who_won'], 'match_won'] = 0

    df['strike_rate'] = (df['runs'] / df['balls_faced']) * 100
    df_i1 = df[(df['innings'] == "bat1") & (df['strike_rate'] >= strike_rate)]
    df_i2 = df[(df['innings'] == "bat2") & (df['strike_rate'] >= strike_rate)]
    mwi1h = df_i1['match_won'].sum()
    win_percentage_inn1 = mwi1h / len(df_i1)
    mwi2h = df_i2['match_won'].sum()
    win_percentage_inn2 = mwi2h / len(df_i2)
    hundered_df = pd.DataFrame()
    hundered_df['innings'] = ["innings1", "innings2"]
    hundered_df['hundreds'] = [len(df_i1), len(df_i2)]
    hundered_df['win_percentage'] = [win_percentage_inn1 * 100, win_percentage_inn2 * 100]

    fig = simple_bar_plot(data=hundered_df, x="innings", y="hundreds",
                          color_column="hundreds")
    st.header("innings wise hundred")
    st.plotly_chart(fig, use_container_width=False)
    fig = simple_bar_plot(data=hundered_df, x="innings", y="win_percentage",
                          color_column="win_percentage")
    st.header("win percentage when  hundred scored in innings 1 vs hundred scored in innings 2")
    st.plotly_chart(fig, use_container_width=False)


def get_batting_details(df, player, batting_df):
    df = df[df['name'] == player]
    df['strike_rate'] = (df['runs'] / df['balls_faced']) * 100

    batting_df = batting_df[batting_df['name'] == player]
    batting_df = batting_df.round(2)
    overview_dict = {key: value[batting_df.index.tolist()[0]] for key, value in batting_df.to_dict().items()}
    col1, col2, col3 = st.columns(3)
    columns = [col1, col2, col3]
    keys = list(overview_dict.keys())
    highest_score = df['runs'].max()

    for i in range(0, len(keys), 3):
        columns[0].metric(keys[i], overview_dict[keys[i]])
        columns[1].metric(keys[i + 1], overview_dict[keys[i + 1]])
        columns[2].metric(keys[i + 2], overview_dict[keys[i + 2]])
    col1.metric("highest score", highest_score)
    agg_df = df.groupby('year').agg({"runs": "sum"}).reset_index()
    fig = simple_bar_plot(data=agg_df, x="year", y="runs", text="runs")
    st.title("Runs year wise")
    st.plotly_chart(fig, use_container_width=True)
    df = df.sort_values(by=['balls_faced', "runs"])
    fig = simple_line(data=df, x="balls_faced", y="runs", title="runs")
    st.title("Runs vs balls")
    st.plotly_chart(fig, use_container_width=True)
    highest_wicket = list(set(list(df.nlargest(5, "runs")['runs'])))
    best_performances = df[df['runs'].isin(highest_wicket)].sort_values(by=["runs"], ascending=False)
    best_performances = best_performances[['runs', "balls_faced", "year", "strike_rate"]]
    best_performances=best_performances.reset_index()
    del best_performances['index']

    st.table(best_performances)


def get_bolwing_details(df, bowling_df, player):
    df = df[df['name'] == player]
    bowling_df = bowling_df[bowling_df['name'] == player]
    bowling_df = bowling_df.round(2)
    overview_dict = {key: value[bowling_df.index.tolist()[0]] for key, value in bowling_df.to_dict().items()}
    col1, col2, col3 = st.columns(3)
    columns = [col1, col2, col3]
    keys = list(overview_dict.keys())
    highest_wicket = list(set(list(df.nlargest(5, "wickets")['wickets'])))
    best_performances = df[df['wickets'].isin(highest_wicket)].sort_values(by=["wickets"], ascending=False)
    agg_df = df.groupby('year').agg({"wickets": "sum"}).reset_index()
    fig = simple_bar_plot(data=agg_df, x="year", y="wickets", text="wickets")
    st.title("Wickets Year Wise")
    st.plotly_chart(fig, use_container_width=True)
    for i in range(0, len(keys), 3):
        columns[0].metric(keys[i], overview_dict[keys[i]])
        columns[1].metric(keys[i + 1], overview_dict[keys[i + 1]])
        columns[2].metric(keys[i + 2], overview_dict[keys[i + 2]])
    st.title("Best Performances")

    best_performances = best_performances[['wickets', "runs_given", "year"]]
    best_performances=best_performances.reset_index()
    del best_performances['index']
    st.table(best_performances)


st.sidebar.image("images/ipl_logo.cms")
option = st.sidebar.selectbox(
    'options',
    ('overall tally', 'year wise trends', "player stats", "insights"))
if option in ['overall tally', 'year wise trends']:
    dataframe = pd.read_parquet("tabular_data/matches_data_agg.parquet")
if option == "overall tally":
    year_wise_options = st.sidebar.selectbox(
        'Year Wise Options', ["points"])
    get_overall_tally(dataframe=dataframe)
elif option == "year wise trends":
    year_wise_options = st.sidebar.selectbox(
        'Year Wise Options', ('score', 'stadium', "boundaries"))
    if year_wise_options == "score":
        get_year_wise_data(df=dataframe)
    if year_wise_options == "stadium":
        option = st.selectbox('stadium', ((i) for i in list(set(dataframe['venue']))))
        get_stadium_stats(df=dataframe, stadium=option)
    if year_wise_options == "boundaries":
        get_boundary_data(df=dataframe)
elif option in ["player stats", "insights"]:
    player_stat = pd.read_parquet("tabular_data/players_stat.parquet")
    try:
        batting_df = pd.read_parquet("tabular_data/agg_batting.parquet")
    except Exception as e:
        print(e)
    try:
        bowling_df = pd.read_parquet("tabular_data/agg_bowling.parquet")
    except Exception as e:
        print(e)

    if option == "player stats":
        player_stat_option = st.sidebar.selectbox(
            'player stats', ('overview', "detail"))
        st.header("Player Stats")
        which_stat = st.radio(
            "which_stat",
            ("batting", "bowling"))
        if player_stat_option == "overview":

            min_matches = st.slider('minimum_mathces', 0, 500, 50)
            if which_stat == "batting":
                average_more_than = st.slider('average more than', 0, 100, 20)
                strike_rate_more_than = st.slider('strike rate more than', 0, 250, 120)
                get_player_stats(min_matches=min_matches, min_average=average_more_than,
                                 min_strike_rate=strike_rate_more_than,
                                 df=player_stat)
            elif which_stat == "bowling":
                economy_less_more_than = st.slider('economy rate less than', 0.0, 12.5, 8.0)
                get_player_stats(max_eco=economy_less_more_than, df=player_stat, min_matches=min_matches)
        elif player_stat_option == "detail":
            player_name = st.selectbox("players", ((i) for i in sorted(list(set(player_stat['name'])))))
            if which_stat == "batting":
                get_batting_details(df=player_stat, player=player_name, batting_df=batting_df)
            if which_stat == "bowling":
                get_bolwing_details(df=player_stat, player=player_name, bowling_df=bowling_df)


    elif option == "insights":
        st.header("100 insights")
        insight = st.sidebar.selectbox(
            'Year Wise Options', ("hundrerds", "fifties"))
        strike_rate = st.slider('strike rate more than ', 100, 250, 120)
        if insight == "hundrerds":
            get_hundred_insights(player_stat, strike_rate=strike_rate, mile_stone="hund")
        else:
            get_hundred_insights(df=player_stat, strike_rate=strike_rate, mile_stone="fifty")
