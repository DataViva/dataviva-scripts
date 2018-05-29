import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def occupations(upload):

    csv = s3.get('metadata/cbo.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            "id": str
        }
    )

    occupations_family = {}
    occupations_group = {}

    for _, row in df.iterrows():
        if len(row['id']) == 1:
            occupation_group = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"]
            }

            if upload != 'only_s3':
                redis.set('occupation_group/' +
                      str(row['id']), pickle.dumps(occupation_group))
            occupations_group[row['id']] = occupation_group

    for _, row in df.iterrows():
        if len(row['id']) == 4:
            occupation_family = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'occupation_group': occupations_group[row['id'][0]],
            }

            if upload != 'only_s3':
                redis.set('occupation_family/' +
                      str(row['id']), pickle.dumps(occupation_family))
            occupations_family[row['id']] = occupation_family

    if upload != 'only_redis':
        s3.put('occupation_family.json', json.dumps(
            occupations_family, ensure_ascii=False))

        s3.put('occupation_group.json', json.dumps(
            occupations_group, ensure_ascii=False))

    click.echo("Occupations loaded.")
