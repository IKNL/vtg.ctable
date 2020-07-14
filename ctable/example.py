from vantage6.tools.mock_client import ClientMockProtocol
from vantage6.tools.container_client import ClientContainerProtocol

import time

## Mock client
client = ClientMockProtocol(["./local/a.csv", "./local/b.csv", "./local/c.csv"],
                             "ct")

# retrieve organizations
# organizations = client.get_organizations_in_my_collaboration()
# print(organizations)
# ids = [organization["id"] for organization in organizations]

# # first organization needs to be doing a little extra
# key_holder = [ids[0]]
# others = ids[1:]

# # compute R and CT
# task = client.create_new_task({"method":"init"}, key_holder)
# results = client.get_results(task.get("id"))
# CT = results[0].get("result")
# while client.get_task(task.get("id")).get("complete") != "true":
#     time.sleep(1)

# results = client.get_results(task.get("id"))
# # all other parties add their CT
# for org_id in others:
#     task = client.create_new_task({"method":"add_table", "args":[CT]}, [org_id])
#     while client.get_task(task.get("id")).get("complete") != "true":
#         time.sleep(1)

#     results = client.get_results(task.get("id"))
#     CT = results[0].get("result")

# # finally remove the random matrix in the orginal node
# task = client.create_new_task({"method":"remove_random_values", "args":[CT]}, key_holder)
# while client.get_task(task.get("id")).get("complete") != "true":
#     time.sleep(1)

# results = client.get_results(task.get("id"))
# print(results)

# import pickle
# from vantage6.common import base64s_to_bytes, bytes_to_base64s
# fr = pickle.loads(base64s_to_bytes(results[0].get("result")))
# print(fr)

from ct.round_robin_ct import master as robin_master
from ct import RPC_get_unique_categories_from_columns
from ct.simple_ct import master as simple_master
result = robin_master(client, "", ["aspiration", "drive_wheels"], ["fuel_type", "body_style"])
# result = RPC_get_unique_categories_from_columns(client.datasets[0])
# print(result)
# result = simple_master(client, "", row=["aspiration", "drive_wheels"], column=["fuel_type", "body_style"])
print(result)

# [x] temporary storage
# [x] master algorithm
# [ ] dockerize it

# master_task = client.create_new_task({"master": 1, "method":"master"}, [ids[0]])
# results = client.get_results(task.get("id"))
# print(results)