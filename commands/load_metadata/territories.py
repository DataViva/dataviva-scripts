import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def territories(upload):
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
        if upload != 'only_s3':
            redis.set('territory/' +
                  str(row['municipy_id']), pickle.dumps(territory))

    if upload != 'only_redis':
        s3.put('territory.json', json.dumps(territories, ensure_ascii=False))

    click.echo("Territories loaded.")
