import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def inflections():
    csv = s3.get('metadata/inflections.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'name_pt', 'gender', 'plural']
    )

    inflections = {}

    for _, row in df.iterrows():
        inflection = {
            'id': row['id'],
            'name_en': row['name_en'],
            'name_pt': row['name_pt'],
            'gender': row['gender'],
            'plural': row['plural']
        }
        inflections[row['id']] = inflection
        redis.set('inflection/' + str(row['id']), pickle.dumps(inflection))

    s3.put('inflection.json', json.dumps(
        inflections, ensure_ascii=False))

    click.echo("Inflections loaded.")
