""" methods.py

This file contains all algorithm pieces that are executed on the nodes.
It is important to note that the master method is also triggered on a
node just the same as any other method.

When a return statement is reached the result is send to the central
server after encryption.
"""
import os
import sys
import time
import json
import pandas as pd
import numpy as np
import pickle

from pathlib import Path

from vantage6.tools.util import info, warn
from vantage6.common import ( bytes_to_base64s, base64s_to_bytes )
from ctable.simple import RPC_compute_ct as compute_ct

from itertools import product

def wait_and_collect_results(client, task_id):
    task = client.get_task(task_id)
    while not task.get("complete"):
        task = client.get_task(task_id)
        info("Waiting for results")
        time.sleep(1)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))
    return results



def master(client, data, row, column):
    """Master algoritm.

    The master algorithm is the chair of the Round Robin, which makes
    sure everyone waits for their turn to identify themselfs.
    """

    # get all organizations (ids) that are within the collaboration
    # FlaskIO knows the collaboration to which the container belongs
    # as this is encoded in the JWT (Bearer token)
    organizations = client.get_organizations_in_my_collaboration()
    ids = [organization.get("id") for organization in organizations]
    # first organization needs to be doing a little extra
    key_holder = [ids[0]]
    others = ids[1:]
    # The input fot the algorithm is the same for all organizations
    # in this case

    lengths_of_each_category = {}
    info("Obtain catergories")
    task = client.create_new_task(
        input_={"method":"get_unique_categories_from_columns"},
        organization_ids=ids
    )
    results = wait_and_collect_results(client, task.get("id"))
    # [
    #       {
    #           "results": {
    #               "colname1": ["item1", "item2", ...],
    #               "colname2": [...]
    #           },
    #           ...
    #       },
    #       {...}
    # ]
    # assume we have the same column names at each site
    categories = [categories.get("result") for categories in results]
    # [
    #       {
    #           "colname1": [...],
    #           "colname2": [...]
    #       }
    #       ...
    # ]
    colnames = categories[0].keys()
    categories_per_column = {}
    for col in colnames:
        categories_from_all_sites = [site.get(col) for site in categories]
        # [[...],[...]]
        # lengths_of_each_category[col] = len(np.unique(np.concatenate(categories_from_all_sites)))
        categories_per_column[col] = np.unique(np.concatenate(categories_from_all_sites))

    # print(categories_per_column)
    info("Defining input parameters")
    input_ = {
        "method": "init",
        "args": [row, column, categories_per_column]
    }

    # create a new task for all organizations in the collaboration.
    info("Dispatching initialization-task")
    task = client.create_new_task(
        input_=input_,
        organization_ids=key_holder
    )

    # wait for node to return results. Instead of polling it is also
    # possible to subscribe to a websocket channel to get status
    # updates
    info("Waiting for resuls")
    task_id = task.get("id")
    results = wait_and_collect_results(client, task_id)
    CT = results[0].get("result")

    info("Defining input parameters")
    input_ = {
        "method": "add_table",
        "args": (CT,row , column)
    }

    # create a new task for all organizations in the collaboration.
    for org in others:
        info("Dispatching add_table-task")
        task = client.create_new_task(
            input_=input_,
            organization_ids=[org]
        )

        info("Waiting for resuls")
        task_id = task.get("id")
        results = wait_and_collect_results(client, task_id)
        CT = results[0].get("result")
        input_["args"] = (CT, row, column)

    input_ = {"method":"remove_random_values", "args":(CT, )}

    info("Dispatching add_table-task")
    task = client.create_new_task(
        input_=input_,
        organization_ids=key_holder
    )
    info("Waiting for resuls")
    task_id = task.get("id")
    results = wait_and_collect_results(client, task_id)
    CT = results[0].get("result")

    info("master algorithm complete")

    # return final CT
    return CT

def RPC_init(df, rows, columns, categories):

    LCT = compute_ct(df, rows, columns)

    # adds missing data with 0s
    LCT = add_missing_data(data=LCT,rows=rows,
                           columns=columns, categories=categories)
    # print(LCT)
    # we need to check if all columns contain all categories
    # 1) check if column or row (from the categories) is present in LCT
    # 2) if not present add it do the dataframe with 0's

    Z_ = 365

    # dimensions would be number of columns times the corrensponding number
    # of categories
    # n = 1
    # for column in columns:
    #     n *= len(categories.get(column))
    #     print(categories.get(column))
    # m = 1
    # for row in rows:
    #     m *= len(categories.get(row))
    m = len(LCT.index)
    n = len(LCT.columns)
    R = np.random.uniform(1, Z_, size = (m, n))
    # print(R)

    tmp_folder = Path(os.environ["TEMPORARY_FOLDER"])
    with open( tmp_folder / "R", 'wb') as fp:
        np.save(fp, R)
    with open(tmp_folder / "Z", "w+") as fp:
        fp.write(f"{Z_}")

    RCT = (LCT + R)
    return RCT

def RPC_get_unique_categories_from_columns(df):
    result = {}
    df_cat = df.select_dtypes(include=['object'])
    for column in df_cat:
        result[column]=df_cat[column].unique()
    return result

def add_missing_data(data, rows, columns, categories):
    row_res = product(*[categories.get(row) for row in rows])
    row_res = list(row_res)
    observed_r = list(data.index)
    column_res = product(*[categories.get(col) for col in columns])
    column_res = list(column_res)
    observed_c = list(data.columns)
    for row in row_res:
        if row not in observed_r:
            data.loc[row,:] = (0,)*len(data.columns)
    for col in column_res:
        if col not in observed_c:
            data[col] = [0]*len(data.index)
    data = data.sort_index(axis=0)
    data = data.sort_index(axis=1)
    return data

def RPC_add_table(df, incoming_table, row, column):
    i_table = incoming_table.fillna(0)
    LCT = compute_ct(df, row, column)
    RCT = (i_table.add(LCT, fill_value=0))
    return RCT

def RPC_remove_random_values(df, incoming_table):
    i_table = incoming_table
    tmp_folder = Path(os.environ["TEMPORARY_FOLDER"])
    with open(tmp_folder / "R", 'rb') as fp:
        R = np.load(fp)
    with open(tmp_folder / "Z", 'rb') as fp:
        Z_ = int(fp.read())

    final_CT = ((i_table - R) % Z_)
    return final_CT