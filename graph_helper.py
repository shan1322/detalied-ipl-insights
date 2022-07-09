import plotly.express as px
import plotly.graph_objects as go


def split_bar_plot(title_1, title_2, x, y1, y2, width=1000, height=600):
    fig = go.Figure(data=[
        go.Bar(name=title_1, x=x, y=y1),
        go.Bar(name=title_2, x=x, y=y2)
    ])
    fig.update_layout(barmode='group', autosize=False,
                      width=width,
                      height=height)
    return fig


def simple_bar_plot(data, x, y, text=None, color_column=None):
    fig = px.bar(data, x=x, y=y, color=color_column, text=text)
    fig.update_layout(autosize=False,
                      width=1000,
                      height=600)
    return fig


def simple_line(data, x, y, title):
    fig = px.line(data, x=x, y=y, title=title)
    fig.update_layout(autosize=False,
                      width=1000,
                      height=600)
    return fig
