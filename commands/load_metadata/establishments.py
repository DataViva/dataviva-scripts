import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def establishments(upload):
    csv = s3.get('metadata/cnes_final.csv')
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

        if upload != 'only_s3':
            redis.set('establishment/' +
                  str(row['id']), pickle.dumps(establishment))

    click.echo("Establishment loaded.")
