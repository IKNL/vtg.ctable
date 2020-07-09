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

from vantage6.tools.util import info, warn
from vantage6.common import ( bytes_to_base64s, base64s_to_bytes )


def master(client, data, *args, **kwargs):
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
    # The input fot the algorithm is the same for all organizations
    # in this case
    info("Defining input parameters")
    input_ = {
        "method": "compute_ct",
        "args": args, #add col names
        "kwargs": kwargs
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
    info("Waiting for resuls")
    task_id = task.get("id")
    task = client.get_task(task_id)
    while not task.get("complete"):
        task = client.get_task(task_id)
        info("Waiting for results")
        time.sleep(1)

    info("Obtaining results")
    results = client.get_results(task_id=task.get("id"))
    local_cts = []
    for result in results:
        local_cts.append(result.get("result"))
    # print(local_cts)
    global_ct = reduce(lambda x, y: x.add(y, fill_value=0), local_cts)
    info("master algorithm complete")

    # return all the messages from the nodes
    return global_ct


def RPC_compute_ct(df,row,column):
    list1=[df[key] for key in row]
    list2=[df[key] for key in column]
    CT = pd.crosstab(list1, list2, dropna=False)
    return CT
