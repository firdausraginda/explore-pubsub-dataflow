import datetime
from google.cloud import bigtable
from setup_gcp import setup_creds
import ast


def write_simple(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    timestamp = datetime.datetime.utcnow()
    column_family_id = "stats_summary"

    row_key = "phone#4c410523#20190501"

    row = table.direct_row(row_key)
    row.set_cell(column_family_id, "connected_cell", 1, timestamp)
    row.set_cell(column_family_id, "connected_wifi", 1, timestamp)
    row.set_cell(column_family_id, "os_build", "PQ2A.190405.003", timestamp)

    row.commit()

    print("Successfully wrote row {}.".format(row_key))


def read_row(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    row_key = "phone#4c410523#20190501"
    row = table.read_row(row_key)

    return row.cells
    # return row.to_dict()


# setup credential gcp
setup_creds()

project_id = 'another-dummy-project-337513'
instance_id = 'dummy-bt'
table_id = 'dummy-table-bt'

# write_simple(project_id, instance_id, table_id)
result = read_row(project_id, instance_id, table_id)
# print(result['stats_summary'])
print(result['stats_summary'][b'connected_cell']['value'])