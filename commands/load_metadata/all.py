import click
import pandas
import pickle
import json
from clients import s3, redis


@click.command()
@click.pass_context
def all(ctx):
    ctx.invoke(sc_course)
    ctx.invoke(ports)
    ctx.invoke(countries)
    ctx.invoke(occupations)
    ctx.invoke(products)
    ctx.invoke(states)
    ctx.invoke(regions)
    ctx.invoke(continents)
    ctx.invoke(territories)
    ctx.invoke(economic_blocks)
    ctx.invoke(municipalities)
    ctx.invoke(industries)
    ctx.invoke(hedu_course)
    ctx.invoke(establishments)
    ctx.invoke(inflections)
    ctx.invoke(metadata_command)
