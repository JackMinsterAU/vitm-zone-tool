#!/usr/bin/env python3
from ..load_data import read_dbf, read_csv
from ..helper_functions import (
    get_fine_zone_dicts,
    record_to_dict,
    distribute_coarse_totals,
    calc_bc_ratios,
    aggregate_zones,
)
import click
import dbf
from typing import List


@click.command()
@click.argument("coarse_dbf", type=click.Path("rb"))
@click.argument("fine_dbf", type=click.Path("rb"))
@click.argument("lookup_csv", type=click.Path("rb"))
@click.argument("output_filename")
def run(coarse_dbf: str, fine_dbf: str, lookup_csv: str, output_filename: str) -> None:
    # read data
    table_coarse = read_dbf(coarse_dbf)
    table_fine = read_dbf(fine_dbf)
    lookup = read_csv(lookup_csv)

    # get list of coarse zones to be replaced
    coarse_zones_to_replace = list(set(lookup["Old_Z_6973"]))

    # loop through each record in the table and copy to the new structure with updates as necessary
    for coarse_zone_record in table_coarse:
        coarse_zone_id = coarse_zone_record.ZONE
        if coarse_zone_id in coarse_zones_to_replace:
            coarse_zone_dict = record_to_dict(table_coarse, coarse_zone_record)
            fine_zone_dicts: List[dict] = get_fine_zone_dicts(
                coarse_zone_id, lookup, table_fine
            )
            fine_zone_dicts = distribute_coarse_totals(
                fine_zone_dicts, coarse_zone_dict
            )
            aggregated_zone_dicts = aggregate_zones(fine_zone_dicts, lookup)
            aggregated_zone_dicts = calc_bc_ratios(aggregated_zone_dicts)

    # create a copy of the structure of the table
    assert output_filename.split(".")[-1] == "dbf"
    custom = table_coarse.new(
        filename=output_filename,
    )

    # write changes to disk
    with custom:
        for coarse_zone_record in table_coarse:
            coarse_zone_id = coarse_zone_record.ZONE
            if coarse_zone_id not in coarse_zones_to_replace:
                custom.append(coarse_zone_record)

        for new_record in aggregated_zone_dicts:
            custom.append(new_record)

        for record in custom:
            dbf.write(record)
