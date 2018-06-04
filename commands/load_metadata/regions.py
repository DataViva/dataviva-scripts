import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def regions(upload):
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
        if upload != 'only_s3':
            redis.set('region/' + str(row['id']), pickle.dumps(region, protocol=2))
            
    if upload != 'only_redis':
        s3.put('region.json', json.dumps(regions, ensure_ascii=False))

    click.echo("Regions loaded.")
