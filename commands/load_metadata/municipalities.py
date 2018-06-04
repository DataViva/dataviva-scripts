import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def municipalities(upload):
    csv = s3.get('metadata/municipalities.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=[
            'uf_id',
            'uf_name',
            'mesorregiao_id',
            'mesorregiao_name',
            'microrregiao_id',
            'microrregiao_name',
            'municipio_id',
            'municipio_name',
            'municipio_id_mdic',
        ],
        converters={
            "uf_id": str,
            "mesorregiao_id": str,
            "microrregiao_id": str,
            "municipio_id": str
        }
    )

    municipalities = {}
    microregions = {}
    mesoregions = {}

    for _, row in df.iterrows():
        municipality = {
            'id': row['municipio_id'],
            'name_pt': row["municipio_name"],
            'name_en': row["municipio_name"],
            'mesoregion': {
                'id': row["mesorregiao_id"],
                'name_pt': row["mesorregiao_name"],
                'name_en': row["mesorregiao_name"],
            },
            'microregion': {
                'id': row["microrregiao_id"],
                'name_pt': row["microrregiao_name"],
                'name_en': row["microrregiao_name"],
            },
            'state': pickle.loads(
                redis.get('state/' + row['municipio_id'][:2])
            ),
            'region': pickle.loads(
                redis.get('region/' + row['municipio_id'][0])
            ),
        }

        municipalities[row['municipio_id']] = municipality
        microregions[row['microrregiao_id']] = municipality['microregion']
        mesoregions[row['mesorregiao_id']] = municipality['mesoregion']

        if upload != 'only_s3':
            redis.set('municipality/' +
                  str(row['municipio_id']), pickle.dumps(municipality, protocol=2))
            redis.set('microregion/' + str(row['microrregiao_id']),
                  pickle.dumps(municipality['microregion'], protocol=2))
            redis.set('mesoregion/' + str(row['mesorregiao_id']),
                  pickle.dumps(municipality['mesoregion'], protocol=2))

    if upload != 'only_redis':
        s3.put('municipality.json', json.dumps(
            municipalities, ensure_ascii=False))

        s3.put('microregion.json', json.dumps(
            microregions, ensure_ascii=False))

        s3.put('mesoregion.json', json.dumps(
            mesoregions, ensure_ascii=False))

    click.echo("Municipalities, microregions and mesoregions loaded.")
