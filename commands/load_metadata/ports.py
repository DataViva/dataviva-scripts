import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def ports():
    csv = s3.get('metadata/ports.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name', 'state']
    )

    ports = {}

    for _, row in df.iterrows():
        port = {
            'name_pt': row["name"] + ' - ' + row["state"],
            'name_en': row["name"] + ' - ' + row["state"]
        }
        ports[row['id']] = port
        redis.set('port/' + str(row['id']), pickle.dumps(port))

    s3.put('port.json', json.dumps(ports, ensure_ascii=False))

    click.echo("Ports loaded.")
