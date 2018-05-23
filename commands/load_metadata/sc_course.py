import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
def sc_course():
    csv = s3.get('metadata/sc_courses.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            "id": str
        }
    )

    sc_courses = {}
    sc_courses_field = {}

    for _, row in df.iterrows():

        if len(row['id']) == 2:
            sc_course_field = {
                'name_pt': row["name_pt"],
                'name_en': row["name_en"]
            }

            redis.set('sc_course_field/' +
                      str(row['id']), pickle.dumps(sc_course_field))
            sc_courses_field[row['id']] = sc_course_field

        elif len(row['id']) == 5:
            sc_course = {
                'name_pt': row["name_pt"],
                'name_en': row["name_en"]
            }

            redis.set('sc_course/' + str(row['id']), pickle.dumps(sc_course))
            sc_courses[row['id']] = sc_course

    s3.put('sc_course.json', json.dumps(sc_courses, ensure_ascii=False))
    s3.put('sc_course_field.json', json.dumps(
        sc_courses_field, ensure_ascii=False))

    click.echo("SC Courses loaded.")
