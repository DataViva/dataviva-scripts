import click
import pandas
import json
from clients import s3, redis


@click.command()
@click.option('--both', 'upload', flag_value='s3_and_redis', default=True, help='Upload metadata to both s3 and Redis')
@click.option('--s3', 'upload', flag_value='only_s3', help='Upload metadata only to s3')
@click.option('--redis', 'upload', flag_value='only_redis', help='Upload metadata only to Redis')
def metadata_command(upload):
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
    ], upload)


def attrs(attrs, upload):
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

        items = {}

        for _, row in df.iterrows():
            item = {
                'id': row["id"],
                'name_pt': row["name_pt"],
                'name_en': row["name_en"],
            }

            items[row['id']] = item
            if upload != 'only_s3':
                redis.set(attr['name'] + '/' + str(row['id']), json.dumps(item, ensure_ascii=False))

        if upload != 'only_redis':
            s3.put(attr['name'] + '.json', json.dumps(items, ensure_ascii=False))

        click.echo(" loaded.")
