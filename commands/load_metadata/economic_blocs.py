import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def economic_blocs(upload):
    csv = s3.get('metadata/economic_blocs.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name', 'country_id'],
        converters={"country_id": str}
    )

    economic_blocs = {}

    for _, row in df.iterrows():

        if economic_blocs.get(row["id"]):
            economic_bloc = economic_blocs[row["id"]]
            economic_bloc["countries"].append(row["country_id"])
        else:
            economic_bloc = {
                'name_en': row["name"],
                'name_pt': row["name"],
                'countries': [
                    row["country_id"]
                ]
            }

        economic_blocs[row['id']] = economic_bloc
        if upload != 'only_s3':
            redis.set('economic_bloc/' + str(row['id']), pickle.dumps(economic_bloc, protocol=2))

    if upload != 'only_redis':
        s3.put('economic_bloc.json', json.dumps(
            economic_blocs, ensure_ascii=False))

    click.echo("Economic Blocs loaded.")
