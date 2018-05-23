import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def regions():
    csv = s3.get('metadata/regions.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'abbr_en', 'name_pt', 'abbr_pt']
    )

    regions = {}

    for _, row in df.iterrows():
        region = {
            'id': row['id'],
            'name_en': row["name_en"],
            'abbr_en': row['abbr_en'],
            'name_pt': row["name_pt"],
            'abbr_pt': row['abbr_pt'],
        }

        regions[row['id']] = region
        redis.set('region/' + str(row['id']), pickle.dumps(region))

    s3.put('region.json', json.dumps(regions, ensure_ascii=False))

    click.echo("Regions loaded.")
