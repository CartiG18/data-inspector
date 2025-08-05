import marimo

__generated_with = "0.14.13"
app = marimo.App(width="medium")


@app.cell
def _():
    '''
    Separate imports from rest of code for clarity
    '''
    import marimo as mo
    import pandas as pd
    import altair as alt
    from pathlib import Path
    import json
    return Path, alt, json, pd


@app.cell
def _(Path, json, pd):
    '''
    Load and flatten json data
    '''

    # Define path
    data_dir = Path("data")

    # Load each JSON file into a pandas DataFrame
    def load_flatten_json(file_path):
        if file_path.stat().st_size == 0:
            raise ValueError(f"{file_path} is empty!")

        with open(file_path, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in {file_path}: {e}")
    
        return pd.json_normalize(data)

    # Load multiple files
    connections_df = load_flatten_json(data_dir/"connection_logs.json")
    play_events_df = load_flatten_json(data_dir/"play_events.json")
    sessions_df = load_flatten_json(data_dir/"customer_sessions.json")
    return (play_events_df,)


@app.cell
def _(play_events_df):
    '''
    Define the dataframe that will be used for visualization
    '''
    result_df = (
            play_events_df
            .groupby("customer_id")
            .agg(avg_duration=("engagement_stats.duration_seconds", "mean"))
            .reset_index()
        )
    return (result_df,)


@app.cell
def _(alt, result_df):
    '''
    Visualization using Altair library
    '''
    chart = (
        alt.Chart(result_df)
        .mark_bar()
        .encode(
            x="customer_id:N",
            y="avg_duration:Q"
        )
        .properties(title="Average Duration by Customer")
    )
    chart.display()
    return


if __name__ == "__main__":
    app.run()
