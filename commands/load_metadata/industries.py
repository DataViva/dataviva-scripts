import click
import pandas
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def industries(upload):
    csv = s3.get('metadata/cnae.csv')
    df = pandas.read_csv(
        csv,
        sep=',',
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            "id": str
        }
    )

    industry_sections = {}
    industry_divisions = {}
    industry_classes = {}

    industry_classes['-1'] = {
        'name_pt': 'Não definido',
        'name_en': 'Undefined'
    }

    industry_sections['0'] = {
        'name_pt': 'Não definido',
        'name_en': 'Undefined'
    }

    for _, row in df.iterrows():
        if len(row['id']) == 1:
            industry_section = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"]
            }

            if upload != 'only_s3':
                redis.set('industry_section/' +
                      str(row['id']), json.dumps(industry_section, ensure_ascii=False))
            industry_sections[row['id']] = industry_section

    for _, row in df.iterrows():
        if len(row['id']) == 3:
            division_id = row['id'][1:3]

            industry_division = {
                'id': division_id,
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'industry_section': row["id"][0]
            }

            if upload != 'only_s3':
                redis.set('industry_division/' + str(division_id),
                      json.dumps(industry_division, ensure_ascii=False))
            industry_divisions[division_id] = industry_division

    for _, row in df.iterrows():
        if len(row['id']) == 6:
            class_id = row["id"][1:]

            industry_classe = {
                'id': class_id,
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'industry_section': industry_sections[row["id"][0]],
                'industry_division': industry_divisions[row["id"][1:3]]
            }

            if upload != 'only_s3':
                redis.set('industry_class/' + str(class_id),
                      json.dumps(industry_classe, ensure_ascii=False))
            industry_classes[class_id] = industry_classe

    if upload != 'only_redis':
        s3.put('industry_class.json', json.dumps(
            industry_classes, ensure_ascii=False))

        s3.put('industry_division.json', json.dumps(
            industry_divisions, ensure_ascii=False))

        s3.put('industry_section.json', json.dumps(
            industry_sections, ensure_ascii=False))

    click.echo("Industries loaded.")
