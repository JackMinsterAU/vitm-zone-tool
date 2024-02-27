# Usage example

Install and run using poetry:
``` sh
poetry install
poetry shell

poetry run zonetool <path_to_coarse_demographic_dbf> \
    <path_to_fine_demographic_dbf> \
    <path_to_lookup_csv> \ 
    <output_dbf_filename>
```

The lookup csv needs to have the following structure: 
| Old_Z_6973           | Z_20825           | New_Z_6973                   |
|:---------------------|:------------------|:-----------------------------|
| <old coarse zone id> | <fine subzone id> | <new id for aggregated zone> |

For example:
| Old_Z_6973 | Z_20825 | New_Z_6973 |
|:-----------|:--------|:-----------|
| 572        | 11292   | 5902       |
| 572        | 11293   | 5902       |
| 572        | 11294   | 5903       |

The coarse zone, 572, will be replaced by new zones 5902 and 5903. 5902 is an aggregate of the fine zones 11292 and 11293. 5903 is just 11294. 
