from vantage6.tools.mock_client import ClientMockProtocol
# from vantage6.tools.container_client import ClientContainer

import time

## Mock client
client = ClientMockProtocol(["./local/a.csv", "./local/b.csv", "./local/c.csv"],
                             "ctable")

# client = ClientMockProtocol(['./local/data.csv', './local/data.csv'], "ctable")

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

from ctable.round_robin import master as robin_master
from ctable import RPC_get_unique_categories_from_columns
from ctable.simple import master as simple_master
# result = robin_master(client, "", ["fuel_type", "num_doors"], ["make", "aspiration"])
result = simple_master(client, "", ["fuel_type", "num_doors"], ["make", "aspiration"])
# result = RPC_get_unique_categories_from_columns(client.datasets[0])
# print(result)
# result = simple_master(client, "", row=["Sex"], column=["Age"])
print(result)

# [x] temporary storage
# [x] master algorithm
# [ ] dockerize it

# master_task = client.create_new_task({"master": 1, "method":"master"}, [ids[0]])
# results = client.get_results(task.get("id"))
# print(results)