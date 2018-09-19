# -*- coding: utf-8 -*-
import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def states(upload):
    csv = s3.get('metadata/uf_ibge_mdic.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['mdic_name', 'mdic_id', 'ibge_id', 'uf', 'old_id'],
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
            'abbr_en': row['uf'], 
            'old_id': row['old_id']
        }

        states[row['ibge_id']] = state
        if upload != 'only_s3':
            redis.set('state/' + str(row['ibge_id']), json.dumps(state, ensure_ascii=False))

    if upload != 'only_redis':
        s3.put('state.json', json.dumps(states, ensure_ascii=False))

    click.echo("States loaded.")
