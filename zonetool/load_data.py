#!/usr/bin/env python3
import dbf
import pandas as pd


def read_dbf(path):
    table = dbf.Table(filename=path)
    table.open(dbf.READ_ONLY)
    return table


def read_csv(path):
    table = pd.read_csv(path)
    return table
