import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def countries(upload):
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

    csv = s3.get('metadata/wld.csv')
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
        if upload != 'only_s3':
            redis.set('country/' + str(row['id']), pickle.dumps(country, protocol=2))

    if upload != 'only_redis':
        s3.put('country.json', json.dumps(countries, ensure_ascii=False))

    click.echo("Countries loaded.")
