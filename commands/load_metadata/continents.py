import click
import pandas
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def continents(upload):
    csv = s3.get('metadata/continents.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'country_id', 'name_en', 'name_pt'],
        converters={
            "country_id": lambda x: '%03d' % int(x)
        }
    )

    continents = {}

    for _, row in df.iterrows():

        if continents.get(row["id"]):
            continent = continents[row["id"]]
            continent["countries"].append(row["country_id"])
        else:
            continent = {
                'countries': [
                    row["country_id"]
                ],
                'name_en': row["name_en"],
                'name_pt': row["name_pt"]
            }

        continents[row['id']] = continent
        if upload != 'only_s3':
            redis.set('continent/' + str(row['id']), json.dumps(continent, ensure_ascii=False))

    if upload != 'only_redis':
        s3.put('continent.json', json.dumps(continents, ensure_ascii=False))

    click.echo("Continents loaded.")
