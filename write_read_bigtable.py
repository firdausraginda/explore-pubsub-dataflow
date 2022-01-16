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


def read_columns_per_row_key(table, row_key, column_family_id):

    row = table.read_row(row_key)
    rows = row.cells[column_family_id]

    list_results = []
    for key in rows.keys():
        dict_temp = {}

        values_temp = []
        values = rows[key]
        for value in values:
            values_temp.append(value.value.decode("utf-8"))
        
        key_decoded = key.decode("utf-8")
        dict_temp[key_decoded] = values_temp
        list_results.append(dict_temp)

    print(list_results)


def read_cell_by_column_id(table, row_key, column_family_id, column_id):

    row = table.read_row(row_key)
    column_id_encoded = column_id.encode("utf-8")
    cols = row.cells[column_family_id][column_id_encoded]
    
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
read_columns_per_row_key(table, 'hobbies_1', 'person_hobbies')
# read_cell_by_column_id(table, 'hobbies_1', 'person_hobbies', 'mike')
