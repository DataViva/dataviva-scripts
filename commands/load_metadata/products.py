import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def products(upload):
    csv = s3.get('metadata/hs.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_pt', 'name_en', 'profundidade_id', 'profundidade'],
        converters={
            "id": str
        }
    )

    products = {}
    product_sections = {}
    product_chapters = {}

    for _, row in df.iterrows():
        if row['profundidade'] == 'Seção':
            product_section_id = row['id']

            product_section = {
                'id': product_section_id,
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
            }

            if upload != 'only_s3':
                redis.set('product_section/' + str(product_section_id),
                      pickle.dumps(product_section))
            product_sections[product_section_id] = product_section

        elif row['profundidade'] == 'Capítulo':
            product_chapter_id = row['id'][2:]

            product_chapter = {
                'id': product_chapter_id,
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
            }

            if upload != 'only_s3':
                redis.set('product_chapter/' + str(product_chapter_id),
                      pickle.dumps(product_chapter))
            product_chapters[product_chapter_id] = product_chapter

    for _, row in df.iterrows():
        if row['profundidade'] == 'Posição':
            product_id = row['id'][2:]
            product_section_id = row["id"][:2]
            product_chapter_id = row["id"][2:4]

            product = {
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'product_section': product_sections[product_section_id],
                'product_chapter': product_chapters[product_chapter_id],
            }

            products[product_id] = product
            if upload != 'only_s3':
                redis.set('product/' + str(product_id), pickle.dumps(product))

    if upload != 'only_redis':
        s3.put('product.json', json.dumps(products, ensure_ascii=False))

        s3.put('product_section.json', json.dumps(
            product_sections, ensure_ascii=False))

        s3.put('product_chapter.json', json.dumps(
            product_chapters, ensure_ascii=False))

    click.echo("Products loaded.")
