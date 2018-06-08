import click
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
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
@click.pass_context
def all(ctx, upload):
    ctx.invoke(continents, upload=upload)
    ctx.invoke(countries, upload=upload)
    ctx.invoke(economic_blocs, upload=upload)
    ctx.invoke(establishments, upload=upload)
    ctx.invoke(hedu_course, upload=upload)
    ctx.invoke(industries, upload=upload)
    ctx.invoke(inflections, upload=upload)
    ctx.invoke(metadata_command, upload=upload)
    ctx.invoke(occupations, upload=upload)
    ctx.invoke(ports, upload=upload)
    ctx.invoke(products, upload=upload)
    ctx.invoke(regions, upload=upload)
    ctx.invoke(sc_course, upload=upload)
    ctx.invoke(states, upload=upload)
    ctx.invoke(municipalities, upload=upload)
    ctx.invoke(territories, upload=upload)
