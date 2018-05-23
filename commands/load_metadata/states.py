import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def states():
    csv = s3.get('metadata/uf_ibge_mdic.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['mdic_name', 'mdic_id', 'ibge_id', 'uf'],
        converters={
            "ibge_id": str
        }
    )

    states = {}

    for _, row in df.iterrows():
        if not row['ibge_id']:
            continue

        state = {
            'id': row['ibge_id'],
            'name_pt': row["mdic_name"],
            'name_en': row["mdic_name"],
            'abbr_pt': row['uf'],
            'abbr_en': row['uf']
        }

        states[row['ibge_id']] = state
        redis.set('state/' + str(row['ibge_id']), pickle.dumps(state))

    s3.put('state.json', json.dumps(states, ensure_ascii=False))

    click.echo("States loaded.")
