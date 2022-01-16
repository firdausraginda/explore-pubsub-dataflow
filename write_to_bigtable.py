import datetime
from google.cloud import bigtable
from setup_gcp import setup_creds


def bigtable_config():

    project_id = 'another-dummy-project-337513'
    instance_id = 'dummy-bt'
    table_id = 'dummy-table-bt'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    return table


def write_simple(table, row_key, column_family_id, data):

    timestamp = datetime.datetime.utcnow()
    row = table.direct_row(row_key)

    for item in data:
        row.set_cell(column_family_id, item['person'], item['hobby'], timestamp)

    row.commit()

    print("Successfully wrote row {}.".format(row_key))


def read_rows(table, row_key, column_family_id, column_id):

    row = table.read_row(row_key.encode("utf-8"))
    column_id = column_id.encode("utf-8")
    value = row.cells[column_family_id][column_id][0].value.decode("utf-8")

    print(value)


# setup credential gcp
setup_creds()

# setup bigtable
table = bigtable_config()

# define data
row_key = 'hobbies_1'
column_family_id = 'person_hobbies'
column_id = 'person'
person_hobbies = [
    {'person': 'mike', 'hobby': 'swimming'},
    {'person': 'jhon', 'hobby': 'dancing'},
    {'person': 'melia', 'hobby': 'cooking'}
]

write_simple(table, row_key, column_family_id, person_hobbies)
# read_rows(project_id, instance_id, table_id)
