import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def establishments():
    csv = s3.get('attrs/cnes_final.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            'id': str,
        }
    )

    for _, row in df.iterrows():

        establishment = {
            'id': row['id'],
            'name_pt': row["name_pt"],
            'name_en': row["name_en"],
        }

        redis.set('establishment/' +
                  str(row['id']), pickle.dumps(establishment))

    click.echo("Establishment loaded.")
