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

from functools import reduce
from pathlib import Path
from itertools import product

from vantage6.tools.util import info, warn
from vantage6.common import ( bytes_to_base64s, base64s_to_bytes )


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
    # categories = [categories.get("result") for categories in results]
    categories = results
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

    # first organization needs to be doing a little extra
    # The input fot the algorithm is the same for all organizations
    # in this case
    info("Defining input parameters")
    input_ = {
        "method": "compute_ct",
        "args": [row, column, categories_per_column],
        "kwargs": {}
    }

    # create a new task for all organizations in the collaboration.
    info("Dispatching initialization-task")
    task = client.create_new_task(
        input_=input_,
        organization_ids=ids
    )

    # wait for node to return results. Instead of polling it is also
    # possible to subscribe to a websocket channel to get status
    # updates
    results = wait_and_collect_results(client, task.get("id"))

    local_cts = []
    # for result in results:
    #     local_cts.append(result.get("result"))
    local_cts = results

    global_ct = reduce(lambda x, y: x.add(y, fill_value=0), local_cts)
    info("master algorithm complete")

    # return all the messages from the nodes
    return global_ct


def RPC_compute_ct(df, row, column, categories=None):
    list1 = [df[key] for key in row]
    list2 = [df[key] for key in column]
    CT = pd.crosstab(list1, list2, dropna=False)
    if categories:
        CT = add_missing_data(data=CT, rows=row,
                            columns=column, categories=categories)
    return CT


def add_missing_data(data, rows, columns, categories):
    row_res = product(*[categories.get(row) for row in rows])
    row_res = list(row_res)
    observed_r = [tuple(elem) for elem in list(data.index)]
    for row in row_res:
        if row not in observed_r:
            data.loc[row,:] = (0,)*len(data.columns)

    column_res = product(*[categories.get(col) for col in columns])
    column_res = list(column_res)
    observed_c = [tuple(elem) for elem in list(data.columns)]
    for col in column_res:

        if col not in observed_c:
            data[col] = [0]*len(data.index)

    data = data.sort_index(axis=0)
    data = data.sort_index(axis=1)
    return data

def wait_and_collect_results(client, task_id):
    task = client.get_task(task_id)
    while not task.get("complete"):
        task = client.get_task(task_id)
        info("Waiting for results")
        time.sleep(1)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))
    return results
