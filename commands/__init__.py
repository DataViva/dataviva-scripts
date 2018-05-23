import click

from commands.load_metadata import sc_course
from commands.load_metadata import ports
from commands.load_metadata import countries
from commands.load_metadata import occupations
from commands.load_metadata import products
from commands.load_metadata import states
from commands.load_metadata import regions
from commands.load_metadata import continents
from commands.load_metadata import territories
from commands.load_metadata import economic_blocs
from commands.load_metadata import municipalities
from commands.load_metadata import industries
from commands.load_metadata import hedu_course
from commands.load_metadata import establishments
from commands.load_metadata import inflections
from commands.load_metadata import attrs
from commands.load_metadata import metadata_command
from commands.load_metadata import all


@click.group()
def load_metadata():
    pass

load_metadata.add_command(sc_course)
load_metadata.add_command(ports)
load_metadata.add_command(countries)
load_metadata.add_command(occupations)
load_metadata.add_command(products)
load_metadata.add_command(states)
load_metadata.add_command(regions)
load_metadata.add_command(continents)
load_metadata.add_command(territories)
load_metadata.add_command(economic_blocs)
load_metadata.add_command(municipalities)
load_metadata.add_command(industries)
load_metadata.add_command(hedu_course)
load_metadata.add_command(establishments)
load_metadata.add_command(inflections)
load_metadata.add_command(metadata_command)
load_metadata.add_command(all)
