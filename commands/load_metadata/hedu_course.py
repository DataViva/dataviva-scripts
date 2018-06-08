import click
import pandas
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def hedu_course(upload):
    csv = s3.get('metadata/hedu_courses.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            "id": str
        }
    )

    hedu_courses = {}
    hedu_courses_field = {}

    for _, row in df.iterrows():
        if len(row['id']) == 2:
            hedu_course_field = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
            }

            if upload != 'only_s3':
                redis.set('hedu_course_field/' +
                      str(row['id']), json.dumps(hedu_course_field, ensure_ascii=False))
            hedu_courses_field[row['id']] = hedu_course_field

    for _, row in df.iterrows():
        if len(row['id']) == 6:
            hedu_course = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'hedu_course_field': hedu_courses_field[row['id'][:2]]
            }

            if upload != 'only_s3':
                redis.set('hedu_course/' +
                      str(row['id']), json.dumps(hedu_course, ensure_ascii=False))
            hedu_courses[row['id']] = hedu_course

    if upload != 'only_redis':
        s3.put('hedu_course.json', json.dumps(
            hedu_courses, ensure_ascii=False))

        s3.put('hedu_course_field.json', json.dumps(
            hedu_courses_field, ensure_ascii=False))

    click.echo("HEDU Courses loaded.")
