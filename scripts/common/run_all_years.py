import click
import os, sys, fnmatch
import re

'''
USAGE:
python run_all_years.py -s 2000 -e 2000 -m secex_export
'''

@click.command()
@click.option('-s', '--start', prompt=True, type=int, help='Start year.')
@click.option('-m', '--mode', prompt='Dataset', help='which dataset to use?', type=click.Choice(['secex', 'secex_export', 'secex_import', 'rais']), required=True)
@click.option('-e', '--end', type=int, help='Name of database eg rais or secex.')
@click.option('-d', '--dir', default='data/', type=click.Path(exists=True), prompt=False, help='Directory for data file.')
def main(start, mode, end, dir):
    end = end or start
    end += 1
    
    for y in range(start, end):
        # if y < 2010: continue
        print; print mode, y; print;
        
        if 'secex_' in mode:
            raw_file = os.path.abspath(os.path.join(dir, mode, "raw/MDIC_{}.rar".format(y)))
            trade_flow = "import" if "import" in mode else "export"
            eci_file = os.path.abspath(os.path.join(dir, mode, "observatory_ecis.csv"))
            pci_file = os.path.abspath(os.path.join(dir, mode, "observatory_pcis.csv"))
            output_dir = os.path.abspath(os.path.join(dir, mode, str(y)))
            year_delta = y - start
            cmd = "python scripts/secex_new/format_raw_data.py {} -t {} -y {} -e {} -p {} -o {}".format(raw_file, trade_flow, y, eci_file, pci_file, output_dir)
            if year_delta > 0:
                growth_dir = os.path.abspath(os.path.join(dir, mode, str(y-1)))
                cmd += " -g {}".format(growth_dir)
            if year_delta > 4:
                growth5_dir = os.path.abspath(os.path.join(dir, mode, str(y-5)))
                cmd += " -g5 {}".format(growth5_dir)
            # print raw_file, trade_flow, eci_file, pci_file, output_dir

        if mode == 'secex':
            raw_export_file = os.path.abspath(os.path.join(dir, mode+"_export", "raw/MDIC_{}.rar".format(y)))
            raw_import_file = os.path.abspath(os.path.join(dir, mode+"_import", "raw/MDIC_{}.rar".format(y)))
            eci_file = os.path.abspath(os.path.join(dir, mode+"_export", "observatory_ecis.csv"))
            pci_file = os.path.abspath(os.path.join(dir, mode+"_export", "observatory_pcis.csv"))
            output_dir = os.path.abspath(os.path.join(dir, mode, str(y)))
            year_delta = y - start
            cmd = "python scripts/secex_monthly/format_raw_data.py {} {} -y {} -e {} -p {} -o {}".format(raw_export_file, raw_import_file, y, eci_file, pci_file, output_dir)
            if year_delta > 0:
                growth_dir = os.path.abspath(os.path.join(dir, mode, str(y-1)))
                cmd += " -g {}".format(growth_dir)
            if year_delta > 4:
                growth5_dir = os.path.abspath(os.path.join(dir, mode, str(y-5)))
                cmd += " -g5 {}".format(growth5_dir)
            # print raw_file, trade_flow, eci_file, pci_file, output_dir
            # print cmd

        if mode == 'rais':
            raw_file = os.path.abspath(os.path.join(dir, mode, "Rais_{}.csv".format(y)))
            output_dir = os.path.abspath(os.path.join(dir, mode, str(y)))
            year_delta = y - start
            cmd = "python scripts/rais/format_raw_data.py {} -y {} -o {}".format(raw_file, y, output_dir)
            if year_delta > 0:
                growth_dir = os.path.abspath(os.path.join(dir, mode, str(y-1)))
                cmd += " -g {}".format(growth_dir)
            if year_delta > 4:
                growth5_dir = os.path.abspath(os.path.join(dir, mode, str(y-5)))
                cmd += " -g5 {}".format(growth5_dir)
        
        os.system(cmd)

'''
scripts/secex_new/format_raw_data.py data/secex_import/raw/MDIC_2000.csv -t import -y 2000 -e data/secex_import/observatory_ecis.csv -p data/secex_import/observatory_pcis.csv -o data/secex_import/2000
'''

if __name__ == '__main__':
    main()
