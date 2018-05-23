import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def occupations():

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

            redis.set('occupation_family/' +
                      str(row['id']), pickle.dumps(occupation_family))
            occupations_family[row['id']] = occupation_family

    s3.put('occupation_family.json', json.dumps(
        occupations_family, ensure_ascii=False))

    s3.put('occupation_group.json', json.dumps(
        occupations_group, ensure_ascii=False))

    click.echo("Occupations loaded.")
