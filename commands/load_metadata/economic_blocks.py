import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def economic_blocs():
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
        redis.set('economic_bloc/' + str(row['id']), pickle.dumps(economic_bloc))

    s3.put('economic_bloc.json', json.dumps(
        economic_blocs, ensure_ascii=False))

    click.echo("Economic Blocks loaded.")
