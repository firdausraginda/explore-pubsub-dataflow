import datetime
from google.cloud import bigtable
from setup_gcp import setup_creds


def bigtable_config():

    project_id = 'another-dummy-project-337513'
    instance_id = 'dummy-instance'
    table_id = 'my-table'

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


def read_row(table, row_key, column_family_id, column_id):

    row = table.read_row(row_key.encode("utf-8"))
    column_id = column_id.encode("utf-8")
    value = row.cells[column_family_id][column_id]

    print(value)
    print(len(value))


def read_cell_by_column_id(table, row_key, column_family_id, column_id):

    row = table.read_row(row_key.encode("utf-8"))
    column_id = column_id.encode("utf-8")
    cols = row.cells[column_family_id][column_id]
    
    values = []
    for col in cols:
        temp_val = col.value.decode("utf-8")
        values.append(temp_val)

    print(column_id, values)


# setup credential gcp
setup_creds()

# setup bigtable
table = bigtable_config()

# define data
row_key = 'hobbies_1'
column_family_id = 'person_hobbies'
person_hobbies = [
    {'person': 'mike', 'hobby': 'swimming'},
    {'person': 'jhon', 'hobby': 'dancing'},
    {'person': 'melia', 'hobby': 'cooking'}
]

# write_simple(table, row_key, column_family_id, person_hobbies)
read_row(table, 'hobbies_1', 'person_hobbies', 'mike')
read_cell_by_column_id(table, 'hobbies_1', 'person_hobbies', 'mike')
