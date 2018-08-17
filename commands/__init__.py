import click

from commands.load_metadata.all import all
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
from commands.load_metadata.universities import universities


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
load_metadata.add_command(universities)
load_metadata.add_command(all)
