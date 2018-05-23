import click
import pandas
import pickle
import json
from clients import s3, redis
from commands.load_metadata.continents import continents
from commands.load_metadata.countries import countries
from commands.load_metadata.economic_blocs import economic_blocs
from commands.load_metadata.establishments import establishments
from commands.load_metadata.hedu_course import hedu_course
from commands.load_metadata.industries import industries
from commands.load_metadata.inflections import inflections
from commands.load_metadata.metadata_command import metadata_command
from commands.load_metadata.municipalities import municipalities
from commands.load_metadata.occupations import occupations
from commands.load_metadata.ports import ports
from commands.load_metadata.products import products
from commands.load_metadata.regions import regions
from commands.load_metadata.sc_course import sc_course
from commands.load_metadata.states import states
from commands.load_metadata.territories import territories


@click.command()
@click.pass_context
def all(ctx):
    ctx.invoke(continents)
    ctx.invoke(countries)
    ctx.invoke(economic_blocs)
    ctx.invoke(establishments)
    ctx.invoke(hedu_course)
    ctx.invoke(industries)
    ctx.invoke(inflections)
    ctx.invoke(metadata_command)
    ctx.invoke(municipalities)
    ctx.invoke(occupations)
    ctx.invoke(ports)
    ctx.invoke(products)
    ctx.invoke(regions)
    ctx.invoke(sc_course)
    ctx.invoke(states)
    ctx.invoke(territories)
