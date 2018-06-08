import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def ports(upload):
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
        if upload != 'only_s3':
            redis.set('port/' + str(row['id']), json.dumps(port, ensure_ascii=False))

    if upload != 'only_redis':
        s3.put('port.json', json.dumps(ports, ensure_ascii=False))

    click.echo("Ports loaded.")
