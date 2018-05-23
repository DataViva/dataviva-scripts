import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def hedu_course():
    csv = s3.get('metadata/hedu_course.csv')
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

            redis.set('hedu_course_field/' +
                      str(row['id']), pickle.dumps(hedu_course_field))
            hedu_courses_field[row['id']] = hedu_course_field

    for _, row in df.iterrows():
        if len(row['id']) == 6:
            hedu_course = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'hedu_course_field': hedu_courses_field[row['id'][:2]]
            }

            redis.set('hedu_course/' +
                      str(row['id']), pickle.dumps(hedu_course))
            hedu_courses[row['id']] = hedu_course

    s3.put('hedu_course.json', json.dumps(
        hedu_courses, ensure_ascii=False))

    s3.put('hedu_course_field.json', json.dumps(
        hedu_courses_field, ensure_ascii=False))

    click.echo("HEDU Courses loaded.")
