import click
import pandas
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def universities(upload):
    csv = s3.get('metadata/universities.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_pt', 'name_en', 'school_type'],
        converters={
            "id": str
        }
    )

    items = {}

    for _, row in df.iterrows():
        item = {
            'id': row["id"],
            'name_pt': row["name_pt"],
            'name_en': row["name_en"],
            'school_type': row["school_type"],
        }

        items[row['id']] = item
        if upload != 'only_s3':
            redis.set('university/' + str(row['id']), json.dumps(item, ensure_ascii=False))

    if upload != 'only_redis':
        s3.put('university.json', json.dumps(items, ensure_ascii=False))

    click.echo("Universities loaded.")