import click, pandas, pickle, json
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

            redis.set('sc_course_field/' + str(row['id']), pickle.dumps(sc_course_field))
            sc_courses_field[row['id']] = sc_course_field

        elif len(row['id']) == 5:
            sc_course = {
                'name_pt': row["name_pt"],
                'name_en': row["name_en"]
            }

            redis.set('sc_course/' + str(row['id']), pickle.dumps(sc_course))
            sc_courses[row['id']] = sc_course

    s3.put('sc_course.json', json.dumps(sc_courses, ensure_ascii=False))
    s3.put('sc_course_field.json', json.dumps(sc_courses_field, ensure_ascii=False))

    click.echo("SC Courses loaded.")

@click.command()
def ports():

    csv = s3.get('metadata/ports.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id','name','state']
    )

    ports = {}

    for _, row in df.iterrows():
        port = {
            'name_pt': row["name"] + ' - ' + row["state"],
            'name_en': row["name"] + ' - ' + row["state"]
        }
        ports[row['id']] = port
        redis.set('port/' + str(row['id']), pickle.dumps(port))

    s3.put('port.json', json.dumps(ports, ensure_ascii=False))

    click.echo("Ports loaded.")

@click.command()
def countries():
    csv = s3.get('metadata/continents.csv')
    df_continents = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'country_id', 'name_en', 'name_pt'],
        converters={
        "country_id": lambda x: '%03d' % int(x)
        }
    )

    continents = {}

    for _, row in df_continents.iterrows():
        continents[row['country_id']] =  {
            'id': row["id"],
            'name_en': row["name_en"],
            'name_pt': row["name_pt"],
        }

    csv = s3.get('metadata/wld.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_pt', 'name_en'],
        converters={
            "id": str
        }
    )

    countries = {}

    for _, row in df.iterrows():
        country = {
            'id': row["id"],
            'name_pt': row["name_pt"],
            'name_en': row["name_en"],
            'continent': continents.get(row["id"], {})
        }

        countries[row['id']] = country
        redis.set('country/' + str(row['id']), pickle.dumps(country))

    s3.put('country.json', json.dumps(countries, ensure_ascii=False))

    click.echo("Countries loaded.")

@click.command()
def occupations():

    csv = s3.get('metadata/cbo.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id','name_en','name_pt'],
        converters={
            "id": str
        }
    )

    occupations_family = {}
    occupations_group = {}

    for _, row in df.iterrows():
        if len(row['id']) == 1:
            occupation_group = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"]
            }

            redis.set('occupation_group/' + str(row['id']), pickle.dumps(occupation_group))
            occupations_group[row['id']] = occupation_group

    for _, row in df.iterrows():
        if len(row['id']) == 4:
            occupation_family = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'occupation_group': occupations_group[row['id'][0]],
            }

            redis.set('occupation_family/' + str(row['id']), pickle.dumps(occupation_family))
            occupations_family[row['id']] = occupation_family

    s3.put('occupation_family.json', json.dumps(occupations_family, ensure_ascii=False))

    s3.put('occupation_group.json', json.dumps(occupations_group, ensure_ascii=False))

    click.echo("Occupations loaded.")

@click.command()
def products():
    csv = s3.get('metadata/hs.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id','name_pt','name_en','profundidade_id','profundidade'],
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

            redis.set('product_section/' + str(product_section_id), pickle.dumps(product_section))
            product_sections[product_section_id] = product_section

        elif row['profundidade'] == 'Capítulo':
            product_chapter_id = row['id'][2:]

            product_chapter = {
                'id': product_chapter_id,
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
            }

            redis.set('product_chapter/' + str(product_chapter_id), pickle.dumps(product_chapter))
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
            redis.set('product/' + str(product_id), pickle.dumps(product))

    s3.put('product.json', json.dumps(products, ensure_ascii=False))

    s3.put('product_section.json', json.dumps(product_sections, ensure_ascii=False))

    s3.put('product_chapter.json', json.dumps(product_chapters, ensure_ascii=False))

    click.echo("Products loaded.")

@click.command()
def states():
    csv = s3.get('metadata/uf_ibge_mdic.csv')
    df = pandas.read_csv(
            csv,
            sep=';',
            header=0,
            names=['mdic_name', 'mdic_id', 'ibge_id', 'uf'],
            converters={
                "ibge_id": str
            }
        )

    states = {}

    for _, row in df.iterrows():
        if not row['ibge_id']:
            continue

        state = {
            'id': row['ibge_id'],
            'name_pt': row["mdic_name"],
            'name_en': row["mdic_name"],
            'abbr_pt': row['uf'],
            'abbr_en': row['uf']
        }

        states[row['ibge_id']] = state
        redis.set('state/' + str(row['ibge_id']), pickle.dumps(state))

    s3.put('state.json', json.dumps(states, ensure_ascii=False))

    click.echo("States loaded.")

@click.command()
def regions():
    csv = s3.get('metadata/regions.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'abbr_en', 'name_pt', 'abbr_pt']
    )

    regions = {}

    for _, row in df.iterrows():
        region = {
            'id': row['id'],
            'name_en': row["name_en"],
            'abbr_en': row['abbr_en'],
            'name_pt': row["name_pt"],
            'abbr_pt': row['abbr_pt'],
        }

        regions[row['id']] = region
        redis.set('region/' + str(row['id']), pickle.dumps(region))

    s3.put('region.json', json.dumps(regions, ensure_ascii=False))

    click.echo("Regions loaded.")

@click.command()
def continents():
    csv = s3.get('metadata/continents.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'country_id', 'name_en', 'name_pt'],
        converters={
            "country_id": lambda x: '%03d' % int(x)
        }
    )

    continents = {}

    for _, row in df.iterrows():

        if continents.get(row["id"]):
            continent = continents[row["id"]]
            continent["countries"].append(row["country_id"])
        else:
            continent = {
                'countries': [
                    row["country_id"]
                ],
                'name_en': row["name_en"],
                'name_pt': row["name_pt"]
            }

        continents[row['id']] = continent
        redis.set('continent/' + str(row['id']), pickle.dumps(continent))

    s3.put('continent.json', json.dumps(continents, ensure_ascii=False))

    click.echo("Continents loaded.")

@click.command()
def territories():
    csv = s3.get('metadata/development_territories.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['territory','microterritory','municipality_id'],
        converters={
            "municipality_id": str
        }
    )

    territories = {}

    for _, row in df.iterrows():
        territory = {
            'territory': row["territory"],
            'microterritory': row["microterritory"],
            'municipality_id': row["municipality_id"]
        }

        territories[row['municipality_id']] = territory
        redis.set('territory/' + str(row['municipality_id']), pickle.dumps(territory))

    s3.put('territory.json', json.dumps(territories, ensure_ascii=False))

    click.echo("Territories loaded.")

@click.command()
def economic_blocs():
    csv = s3.get('metadata/economic_blocs.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id','name','country_id'],
        converters={
            "country_id": str
        }
    )

    economic_blocs = {}

    for _, row in df.iterrows():

        if economic_blocs.get(row["id"]):
            economic_bloc = economic_blocs[row["id"]]
            economic_bloc["countries"].append(row["country_id"])
        else:
            economic_bloc = {
                'name_en': row["name"],
                'name_pt': row["name"],
                'countries': [
                    row["country_id"]
                ]
            }

        economic_blocs[row['id']] = economic_bloc
        redis.set('economic_bloc/' + str(row['id']), pickle.dumps(economic_bloc))

    s3.put('economic_bloc.json', json.dumps(economic_blocs, ensure_ascii=False))

    click.echo("Economic Blocs loaded.")

@click.command()
def municipalities():
    csv = s3.get('metadata/municipalities.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['uf_id', 'uf_name', 'mesorregiao_id', 'mesorregiao_name', 'microrregiao_id', 'microrregiao_name', 'municipio_id', 'municipio_name', 'municipio_id_mdic'],
        converters={
            "uf_id": str,
            "mesorregiao_id": str,
            "microrregiao_id": str,
            "municipio_id": str
        }
    )

    municipalities = {}
    microregions = {}
    mesoregions = {}

    for _, row in df.iterrows():
        municipality = {
            'id': row['municipio_id'],
            'name_pt': row["municipio_name"],
            'name_en': row["municipio_name"],
            'mesoregion': {
                'id': row["mesorregiao_id"],
                'name_pt': row["mesorregiao_name"],
                'name_en': row["mesorregiao_name"],
            },
            'microregion': {
                'id': row["microrregiao_id"],
                'name_pt': row["microrregiao_name"],
                'name_en': row["microrregiao_name"],
            },
            'state': pickle.loads(redis.get('state/' + row['municipio_id'][:2])),
            'region': pickle.loads(redis.get('region/' + row['municipio_id'][0])),
        }

        municipalities[row['municipio_id']] = municipality
        microregions[row['microrregiao_id']] = municipality['microregion']
        mesoregions[row['mesorregiao_id']] = municipality['mesoregion']

        redis.set('muLoadIndustriesnicipality/' + str(row['municipio_id']), pickle.dumps(municipality))
        redis.set('microregion/' + str(row['microrregiao_id']), pickle.dumps(municipality['microregion']))
        redis.set('mesoregion/' + str(row['mesorregiao_id']), pickle.dumps(municipality['mesoregion']))

    s3.put('municipality.json', json.dumps(municipalities, ensure_ascii=False))

    s3.put('microregion.json', json.dumps(microregions, ensure_ascii=False))

    s3.put('mesoregion.json', json.dumps(mesoregions, ensure_ascii=False))

    click.echo("Municipalities, microregions and mesoregions loaded.")

@click.command()
def industries():
    csv = s3.get('metadata/cnae.csv')
    df = pandas.read_csv(
        csv,
        sep=',',
        header=0,
        names=['id','name_en','name_pt'],
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

            redis.set('industry_section/' + str(row['id']), pickle.dumps(industry_section))
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


            redis.set('industry_division/' + str(division_id), pickle.dumps(industry_division))
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

            redis.set('industry_class/' + str(class_id), pickle.dumps(industry_classe))
            industry_classes[class_id] = industry_classe

    s3.put('industry_class.json', json.dumps(industry_classes, ensure_ascii=False))

    s3.put('industry_division.json', json.dumps(industry_divisions, ensure_ascii=False))

    s3.put('industry_section.json', json.dumps(industry_sections, ensure_ascii=False))

    click.echo("Industries loaded.")

@click.command()
def hedu_course():
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

            redis.set('hedu_course_field/' + str(row['id']), pickle.dumps(hedu_course_field))
            hedu_courses_field[row['id']] = hedu_course_field

    for _, row in df.iterrows():
        if len(row['id']) == 6:
            hedu_course = {
                'id': row['id'],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
                'hedu_course_field': hedu_courses_field[row['id'][:2]]
            }

            redis.set('hedu_course/' + str(row['id']), pickle.dumps(hedu_course))
            hedu_courses[row['id']] = hedu_course

    s3.put('hedu_course.json', json.dumps(hedu_courses, ensure_ascii=False))

    s3.put('hedu_course_field.json', json.dumps(hedu_courses_field, ensure_ascii=False))

    click.echo("HEDU Courses loaded.")

@click.command()
def establishments():
    csv = s3.get('attrs/cnes_final.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id', 'name_en', 'name_pt'],
        converters={
            'id': str,
        }
    )

    for _, row in df.iterrows():

        establishment = {
            'id': row['id'],
            'name_pt': row["name_pt"],
            'name_en': row["name_en"],
        }

        redis.set('establishment/' + str(row['id']), pickle.dumps(establishment))

    click.echo("Establishment loaded.")

@click.command()
def inflections():
    csv = s3.get('metadata/inflections.csv')
    df = pandas.read_csv(
        csv,
        sep=';',
        header=0,
        names=['id','name_en','name_pt','gender','plural']
    )

    inflections = {}

    for _, row in df.iterrows():
        inflection = {
            'id': row['id'],
            'name_en': row['name_en'],
            'name_pt': row['name_pt'],
            'gender': row['gender'],
            'plural': row['plural']
        }
        inflections[row['id']] = inflection
        redis.set('inflection/' + str(row['id']), pickle.dumps(inflection))

    s3.put('inflection.json', json.dumps(inflections, ensure_ascii=False))

    click.echo("Inflections loaded.")

def attrs(attrs):
    for attr in attrs:
        click.echo('Loading %s ...' % attr['name'])
        csv = s3.get('metadata/%s' % attr['csv_filename'])
        df = pandas.read_csv(
            csv,
            sep=';',
            header=0,
            converters={
                'id': str
            },
            engine='c'
        )

        items = '{'

        for _, row in df.iterrows():
            item = {
                'id': row["id"],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
            }

            if items == '{':
                items = '{}\"{}\": {}'.format(items, row['id'], json.dumps(item, ensure_ascii=False))
            else:
                items = '{}, \"{}\": {}'.format(items, row['id'], json.dumps(item, ensure_ascii=False))

            redis.set(attr['name'] + '/' + str(row['id']), pickle.dumps(item))

        items = items + '}'

        s3.put('' + attr['name'] + '.json', items)

        click.echo(" loaded.")

@click.command()
def metadata_command():
    attrs([

                #hedu
        {'name': 'shift', 'csv_filename': 'shifts.csv'},
        {'name': 'funding_type', 'csv_filename': 'funding_types.csv'},
        {'name': 'school_type', 'csv_filename': 'school_type.csv'},
        #rais and scholar
        {'name': 'ethnicity', 'csv_filename': 'ethnicities.csv'},
        #rais
        {'name': 'gender', 'csv_filename': 'genders.csv'},
        {'name': 'establishment_size', 'csv_filename': 'establishment_size.csv'},
        {'name': 'literacy', 'csv_filename': 'literacy.csv'},
        {'name': 'simple', 'csv_filename': 'simple.csv'},
        {'name': 'legal_nature', 'csv_filename': 'legal_natures.csv'},
        #sc
        {'name': 'university', 'csv_filename': 'universities.csv'},
        {'name': 'sc_school', 'csv_filename': 'schools.csv'},
        {'name': 'administrative_dependency', 'csv_filename': 'administrative_dependencies.csv'},
        #cnes bed
        {'name': 'bed_type', 'csv_filename': 'hospital_bed_types.csv'},
        {'name': 'bed_type_per_specialty', 'csv_filename': 'cnes_bed_type_per_specialty.csv'},
        #cnes
        {'name': 'cnes_pf_pj', 'csv_filename': 'cnes_pf_pj.csv'},
        # cnes establishment
        {'name': 'establishment_type', 'csv_filename': 'cnes_establishment_type.csv'},
        {'name': 'unit_type', 'csv_filename': 'cnes_unit_type.csv'},
        {'name': 'hierarchy_level', 'csv_filename': 'cnes_hierarchy_level.csv'},
        {'name': 'tax_withholding', 'csv_filename': 'cnes_tax_withholding.csv'},
        {'name': 'administrative_sphere', 'csv_filename': 'cnes_administrative_sphere.csv'},
        {'name': 'selective_waste_collection', 'csv_filename': 'cnes_selective_waste_collection.csv'},
        {'name': 'hospital_care', 'csv_filename': 'cnes_hospital_care.csv'},
        {'name': 'neonatal_unit_facility', 'csv_filename': 'cnes_neonatal_unit_facility.csv'},
        {'name': 'niv_dep_1', 'csv_filename': 'cnes_niv_dep_1.csv'},
        {'name': 'ambulatory_care_facility', 'csv_filename': 'cnes_ambulatory_care_facility.csv'},
        {'name': 'emergency_facility', 'csv_filename': 'emergency_facilities.csv'},
        {'name': 'hospital_attention', 'csv_filename': 'cnes_hospital_attention.csv'},
        {'name': 'ambulatory_attention', 'csv_filename': 'cnes_ambulatory_attention.csv'},
        {'name': 'provider_type', 'csv_filename': 'cnes_provider_type.csv'},
        {'name': 'sus_bond', 'csv_filename': 'sus_bond.csv'},
        {'name': 'high_complexity_hospitals', 'csv_filename': 'cnes_high_complexity_hospitals.csv'},
        {'name': 'medium_complexity_hospitals', 'csv_filename': 'cnes_medium_complexity_hospitals.csv'},
        {'name': 'hospitalization_hospitals', 'csv_filename': 'cnes_hospitalization_hospitals.csv'},
        {'name': 'high_complexity_ambulatories', 'csv_filename': 'cnes_high_complexity_ambulatories.csv'},
        {'name': 'medium_complexity_ambulatories', 'csv_filename': 'cnes_medium_complexity_ambulatories.csv'},
        {'name': 'basic_attention_ambulatories', 'csv_filename': 'cnes_basic_attention_ambulatories.csv'},
        {'name': 'urgency_type', 'csv_filename': 'cnes_urgency_type.csv'},
        {'name': 'sadt_type', 'csv_filename': 'cnes_sadt_type.csv'},
        {'name': 'ambulatory_type', 'csv_filename': 'cnes_ambulatory_type.csv'},
        {'name': 'hospitalization_type', 'csv_filename': 'cnes_hospitalization_type.csv'},
        {'name': 'obstetrical_center_facility', 'csv_filename': 'cnes_obstetrical_center_facility.csv'},
        {'name': 'surgery_center_facility', 'csv_filename': 'cnes_surgery_center_facility.csv'},
        {'name': 'health_region', 'csv_filename': 'cnes_health_region.csv'},
        #cnes equipment
        {'name': 'sus_availability_indicator', 'csv_filename': 'cnes_sus_availability_indicator.csv'},
        {'name': 'equipment_code', 'csv_filename': 'cnes_equipment_code.csv'},
        {'name': 'equipment_type', 'csv_filename': 'cnes_equipment_type.csv'},
        # cnes professionals
        {'name': 'professional_link', 'csv_filename': 'professional_links.csv'},
        {'name': 'sus_healthcare_professional', 'csv_filename': 'cnes_sus_healthcare_professional.csv'},
        #comum
    ])

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
    ctx.invoke(economic_blocs)
    ctx.invoke(municipalities)
    ctx.invoke(industries)
    ctx.invoke(hedu_course)
    ctx.invoke(establishments)
    ctx.invoke(inflections)
    ctx.invoke(metadata_command)
