#!/usr/bin/env python3
import dbf
import pandas as pd
from typing import List, Dict
import copy
from itertools import chain


# function to get all fine zone dicts replacing a coarse zone
def get_fine_zone_dicts(
    coarse_zone: int, lookup: pd.DataFrame, table_fine: dbf.tables.Db3Table
) -> List[dict]:
    fine_zones = [
        row["Z_20825"]
        for _, row in lookup.iterrows()
        if row["Old_Z_6973"] == coarse_zone
    ]
    fine_zone_records = [rec for rec in table_fine if rec.ZONE in fine_zones]
    fine_zone_dicts = [record_to_dict(table_fine, rec) for rec in fine_zone_records]
    return fine_zone_dicts


# convert a dbf record to a python dictionary
def record_to_dict(table: dbf.tables.Db3Table, record: dbf.tables.Record) -> Dict:
    record_dict = {
        field_name: getattr(record, field_name) for field_name in table.field_names
    }
    return record_dict


# distribute the coarse-zone totals (eg. Population) amongst the fine
# zones by proportion of the same
# total in each fine zone
def distribute_coarse_totals(
    fine_zone_dicts: List[dict], coarse_zone_dict: dict
) -> List[dict]:
    keys = list(coarse_zone_dict.keys())
    updated_fine_zone_dicts = copy.deepcopy(fine_zone_dicts)

    for key in keys:
        if key.startswith("BC"):
            postfix = key.split("_")[1]
            employment = "EMP_" + postfix
            temp_bc_key = "temp_" + key + "_total"
            keys.append(temp_bc_key)
            coarse_zone_dict[temp_bc_key] = (
                coarse_zone_dict[key] * coarse_zone_dict[employment]
            )
            for fzd, ufzd in zip(fine_zone_dicts, updated_fine_zone_dicts):
                ufzd[temp_bc_key] = ufzd[key] * ufzd[employment]
                fzd[temp_bc_key] = fzd[key] * fzd[employment]

        elif key == "ZONE":
            pass
        else:
            fine_sum = sum([fzd[key] for fzd in fine_zone_dicts])
            coarse_total = coarse_zone_dict[key]
            for ufzd in updated_fine_zone_dicts:
                ufzd[key] = fine_sum and (ufzd[key] * coarse_total) / (fine_sum) or 0

    return updated_fine_zone_dicts


# calculate the ratio of blue-collar jobs for each _IC postfix
def calc_bc_ratios(aggregated_zone_dicts: List[dict]) -> List[dict]:
    starts_with_temp = lambda key: key.startswith("temp_")
    keys = list(aggregated_zone_dicts[0].keys())
    keys = list(filter(starts_with_temp, keys))
    updated_zone_dicts = []

    for d in aggregated_zone_dicts:
        for key in keys:
            replacement_key = "_".join(key.split("_")[1:3])
            ic_postfix = key.split("_")[2]
            employment_key = "EMP_" + ic_postfix
            d[replacement_key] = d[employment_key] and d[key] / d[employment_key] or 0
            del d[key]
        updated_zone_dicts.append(d)

    return updated_zone_dicts


# for each key sum the values in each dict and return one single dict
# (dicts should all contain the same keys)
def sum_dicts(dicts: List[dict]) -> dict:
    summed_dict = {
        key: sum(d.get(key, 0) for d in dicts) for key in set().union(*dicts)
    }
    return summed_dict


# group together fine zones into sub-coarse zones, according to the lookup table
def aggregate_zones(fine_zone_dicts: List[dict], lookup: pd.DataFrame) -> List[dict]:
    new_zone_ids = list(set(lookup["New_Z_6973"]))
    aggregated_zone_dicts = []
    for zone in new_zone_ids:
        fine_zone_ids = list(lookup[lookup["New_Z_6973"] == zone]["Z_20825"])
        fine_zones_to_aggregate = [
            fzd for fzd in fine_zone_dicts if fzd["ZONE"] in fine_zone_ids
        ]
        aggregated_dict = sum_dicts(fine_zones_to_aggregate)
        aggregated_dict["ZONE"] = zone
        aggregated_zone_dicts.append(aggregated_dict)

    return aggregated_zone_dicts


def combine_and_sort_dicts_by_zone(list_of_dict_lists: List[List[dict]]) -> List[dict]:
    combined_list = list(chain.from_iterable(list_of_dict_lists))
    sorted_list = sorted(combined_list, key=lambda d: int(d["ZONE"]))
    return sorted_list


def create_zeroed_zones(zone_id_list: List[str], table: dbf.tables.Db3Table):
    fieldnames = table.field_names
    zone_dict_list = []

    for zone_id in zone_id_list:
        zone_dict = {field: 0 for field in fieldnames}
        zone_dict["ZONE"] = float(zone_id)
        zone_dict_list.append(zone_dict)
    return zone_dict_list
