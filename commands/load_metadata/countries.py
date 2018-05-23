import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def countries():
    csv = s3.get('metadata/continents.csv')
    df_continents = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'country_id', 'name_en', 'name_pt'],
        converters={
            "country_id": lambda x: '%03d' % int(x)
        }
    )

    continents = {}

    for _, row in df_continents.iterrows():
        continents[row['country_id']] = {
            'id': row["id"],
            'name_en': row["name_en"],
            'name_pt': row["name_pt"],
        }

    csv = s3.get('metadata/attrs_wld.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_pt', 'name_en'],
        converters={
            "id": str
        }
    )

    countries = {}

    for _, row in df.iterrows():
        country = {
            'id': row["id"],
            'name_pt': row["name_pt"],
            'name_en': row["name_en"],
            'continent': continents.get(row["id"], {})
        }

        countries[row['id']] = country
        redis.set('country/' + str(row['id']), pickle.dumps(country))

    s3.put('country.json', json.dumps(countries, ensure_ascii=False))

    click.echo("Countries loaded.")
