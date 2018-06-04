import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def inflections(upload):
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

        if upload != 'only_s3':
            redis.set('inflection/' + str(row['id']), pickle.dumps(inflection))

    if upload != 'only_redis':
        s3.put('inflection.json', json.dumps(
            inflections, ensure_ascii=False))

    click.echo("Inflections loaded.")
