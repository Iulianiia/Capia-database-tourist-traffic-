from asyncio.windows_events import NULL

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from src.db_connect import ps_connect

def import_data (table_name):
    conn = ps_connect()
    cursor = conn.cursor()
    query = f"SELECT * FROM {table_name}"
    df = pl.read_database(query, conn)
    return df



def plot_tromso_traffic(df):
    
    df = df.with_columns(pl.col("date").dt.month().alias("month"))
    fig = go.Figure()

    df_a = df.filter(pl.col("arrival_departure" ) == "A")
    df_monthly_a = df_a.group_by("month").agg(pl.col("passengers").mean()).sort("month")

    df_d = df.filter(pl.col("arrival_departure") == "D")
    df_monthly_d = df_d.group_by("month").agg(pl.col("passengers").mean()).sort("month")


    pass_a = df_monthly_a["passengers"].to_numpy()
    pass_d = df_monthly_d["passengers"].to_numpy()
    pass_diff =pass_a - pass_d

    fig.add_trace(go.Scatter(
        x=df_monthly_a["month"],
        y=df_monthly_a["passengers"],
        name="Arrival",
        visible=True
    ))

    fig.add_trace(go.Scatter(
        x=df_monthly_d["month"],
        y=df_monthly_d["passengers"],
        name="Departure",
        visible=False
    ))
    fig.add_trace(go.Scatter(
        x= df_monthly_a["month"],
        y=pass_diff,
        name="Growth",
        visible=False
    ))

    # dropdown
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=[
                    dict(
                        label="Arrival",
                        method="update",
                        args=[{"visible": [True, False, False]},
                              {"title.text": "Average Monthly Passengers ARRIVING to Tromsø"}]
                    ),
                    dict(
                        label="Departure",
                        method="update",
                        args=[{"visible": [False, True, False]},
                            {"title.text": "Average Monthly Passengers DEPARTING from Tromsø"}]
                    ),
                    dict(
                        label="Both",
                        method="update",
                        args=[{"visible": [True, True, False]},
                            {"title.text": "Arrival vs Departure Tromsø"}]
                    ),
                    dict(
                        label="Growth",
                        method="update",
                        args=[{"visible": [False, False, True]},
                            {"title.text": "Growth of Passengers in Tromsø"}]
                    )
                ],
                direction="down"
            )
        ],
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(1, 13)),
            ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        ),
        xaxis_title="Month",
        yaxis_title="Average Passengers"
    )
    fig.show()


def plot_annual_tromso(df):
    df_a = df.filter(pl.col("arrival_departure" ) == "A")
    df_a_i_sum = df_a.filter(pl.col("international_domestic") == "I").sum() 
    df_a_d_sum = df_a.filter(pl.col("international_domestic") == "D").sum()
    df_a = df_a.with_columns(pl.col("date").dt.year().alias("year"))
    df_annual_sum_a = (
        df_a.group_by("year")
        .agg(pl.col("passengers").sum().alias("annual_passengers"))
        .sort("year")
    )

    df_d = df.filter(pl.col("arrival_departure") == "D")
    df_d_i_sum = df_d.filter(pl.col("international_domestic") == "I").sum()
    df_d_d_sum = df_d.filter(pl.col("international_domestic") == "D").sum()
    df_d = df_d.with_columns(pl.col("date").dt.year().alias("year"))
    df_annual_sum_d = (
        df_d.group_by("year")
        .agg(pl.col("passengers").sum().alias("annual_passengers"))
        .sort("year")
    )
    
    pass_a_annual = df_annual_sum_a["annual_passengers"].to_numpy()
    pass_d_annual = df_annual_sum_d["annual_passengers"].to_numpy()
    pass_diff_annual = pass_a_annual - pass_d_annual

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_annual_sum_a["year"],
        y=df_annual_sum_a["annual_passengers"],
        name="Arrival",
        visible=True
    ))
    fig.add_trace(go.Bar(
        x=df_annual_sum_d["year"],
        y=df_annual_sum_d["annual_passengers"],
        name="Departure",
        visible=False
    ))
    fig.add_trace(go.Scatter(
        x=df_annual_sum_a["year"],
        y=pass_diff_annual,
        name="Growth",
        visible=False
    ))
    fig.add_trace(go.Pie(
        labels=["International", "Domestic"],
        values=[df_a_i_sum["passengers"][0], df_a_d_sum["passengers"][0]],
        name="Arrival",
        visible=False,
        domain=dict(x=[0.5, 1], y=[0.5, 1])))

    fig.add_trace(go.Pie(
        labels=["International", "Domestic"],
        values=[df_d_i_sum["passengers"][0], df_d_d_sum["passengers"][0]],
        name="Departure",
        visible=False,
        domain=dict(x=[0, 0.5], y=[0.5, 1])))

     # dropdown 
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=[
                    dict(
                        label="Arrival",
                        method="update",
                        args=[{"visible": [True, False, False,False, False]},
                              {"title.text": "Annual Passengers ARRIVING to Tromsø"}]
                    ),
                    dict(
                        label="Departure",
                        method="update",
                        args=[{"visible": [False, True, False,False,False]},
                            {"title.text": "Annual Passengers DEPARTING from Tromsø"}]
                    ),
                    dict(
                        label="Both",
                        method="update",
                        args=[{"visible": [True, True, False,False,False]},
                            {"title.text": "Annual Arrival vs Departure Tromsø"}]
                    ),
                    dict(
                        label="Growth",
                        method="update",
                        args=[{"visible": [False, False, True,False,False]},
                            {"title.text": "Growth of Annual Passengers in Tromsø"}]
                    ),
                    dict(
                        label="Arrival vs Departure Breakdown",
                        method="update",
                        args=[{"visible": [False, False, False,True,True]},
                            {"title.text": "Arrival vs Departure Passengers Breakdown",
                                "xaxis": {"visible": False},
                                "yaxis": {"visible": False}
                            }]
                    )
                ], 
                direction="down"
            )
        ],
        xaxis_title="Year",
        yaxis_title="Annual Passengers"
    )
    fig.show()

def plan_aircraft_occupancy(df):
    df_hist = (
    df
    .filter(pl.col("seats").is_not_null() &  (pl.col("seats") > 10) & (pl.col("passengers") > 10) & (pl.col("passengers").is_not_null()))
    .with_columns((pl.col("passengers") / pl.col("seats")).alias("occupancy"))
    )

    fig1 = go.Figure()

    fig1.add_trace(go.Histogram(
        x=df_hist["occupancy"],
        histnorm="density"
    ))

    fig1.update_layout(
        title="How much planes flying from and to Tromsø are usually loaded",
        xaxis_title="Occupancy",
        yaxis_title="Density"
    )

    fig1.show()
    df_month = (
        df
        .filter(pl.col("seats").is_not_null())
        .with_columns(pl.col("date").dt.month().alias("month"))
        .group_by("month")
        .agg(
            (pl.col("passengers").mean() / pl.col("seats").mean()).alias("occupancy")
        )
        .sort("month")
    )

    month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    fig2 = go.Figure()

    fig2.add_trace(go.Bar(
        x=df_month["month"],
        y=df_month["occupancy"]
    ))

    fig2.update_layout(
        title="Monthly Occupancy",
        xaxis=dict(tickmode="array", tickvals=list(range(1,13)), ticktext=month_labels),
        yaxis_title="Occupancy"
    )
    fig2.show()


if __name__ == "__main__":
    df_flights = import_data("ssb_airport_monthly_traffic")
    # Tromsø
    df_flights = df_flights.filter((pl.col("date").is_between(pl.date(2020, 1, 1), pl.date(2026, 1, 1))) & (pl.col("airport_icao_code") == "ENTC") )
   
    plot_tromso_traffic(df_flights)
    plot_annual_tromso(df_flights)
    plan_aircraft_occupancy(df_flights)