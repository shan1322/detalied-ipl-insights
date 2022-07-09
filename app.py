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
        st.table(agg_df)
    else:

        agg_df = df.groupby('name').agg(
            {"wickets": "sum", "team": "count", "runs_given": "sum", "overs": "sum"}).rename(
            columns={"team": "matches"}).round(0).reset_index()
        agg_df['economy'] = agg_df['runs_given'] / agg_df['overs']
        agg_df = agg_df.sort_values(by=['wickets', 'economy'], ascending=False)
        agg_df.to_parquet("tabular_data/agg_bowling.parquet")
        agg_df['economy'] = agg_df['runs_given'] / agg_df['overs']
        agg_df = agg_df[(agg_df['matches'] >= min_matches) & (agg_df['economy'] <= max_eco)]
        st.write(agg_df)


def get_hundred_insights(df, strike_rate):
    df = df[df['hund'] == 1]
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
    if option == "player stats":
        st.header("Player Stats")
        which_stat = st.radio(
            "which_stat",
            ("batting", "bowling"))
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
    elif option == "insights":
        st.header("100 insights")
        insight = st.sidebar.selectbox(
            'Year Wise Options', ("hundrerds", "fifties"))
        strike_rate = st.slider('strike rate more than ', 100, 250, 120)
        get_hundred_insights(player_stat, strike_rate=strike_rate)
