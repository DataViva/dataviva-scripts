import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def territories():
    csv = s3.get('metadata/development_territories.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['territory', 'microterritory', 'municipy_id'],
        converters={
            "municipy_id": str
        }
    )

    territories = {}

    for _, row in df.iterrows():
        territory = {
            'territory': row["territory"],
            'microterritory': row["microterritory"],
            'municipy_id': row["municipy_id"]
        }

        territories[row['municipy_id']] = territory
        redis.set('territory/' +
                  str(row['municipy_id']), pickle.dumps(territory))

    s3.put('territory.json', json.dumps(territories, ensure_ascii=False))

    click.echo("Territories loaded.")
