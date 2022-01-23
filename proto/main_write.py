import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).absolute().parent.parent))
import datetime
from google.cloud import bigtable
from setup_gcp import setup_creds
import person_hobbies_pb2


def bigtable_config():

    project_id = 'another-dummy-project-337513'
    instance_id = 'dummy-instance'
    table_id = 'my-table'

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    return table


def write_simple(table, row_key, column_family_id, dataset):

    person_hobbies = person_hobbies_pb2.PersonHobbies() # define `person hobbies` proto object
    timestamp = datetime.datetime.utcnow()
    row = table.direct_row(row_key)

    for data in dataset:
    
        person = person_hobbies_pb2.Person() # define temporary `person` proto object
        person.name = data['person']
        person.age = data['age']
        
        for hobby in data['hobbies']:
            person.hobbies.add(name=hobby)

        person_hobbies.person.append(person)
        person_hobbies_in_bytes = person_hobbies.SerializeToString() # convert proto object to bytes

        row.set_cell(column_family_id, 'data_1', person_hobbies_in_bytes, timestamp)

    row.commit()

    print("Successfully wrote row {}.".format(row_key))


def read_columns_per_row_key(table, row_key, column_family_id):

    # get row based on given `row_key` & `column_family_id`
    row = table.read_row(row_key)
    rows = row.cells[column_family_id]

    # loop over `column_id` in that row
    list_results = []
    for key in rows.keys():

        # loop over values in a `column_id`
        values_temp = []
        values = rows[key]
        for value in values:
            values_temp.append(value.value.decode("utf-8"))

        # create dictionary with key=`column_id` & value=values in a `column_id`
        dict_temp = {}        
        key_decoded = key.decode("utf-8")
        dict_temp[key_decoded] = values_temp
        list_results.append(dict_temp)

    print(list_results)


def read_cell_by_column_id(table, row_key, column_family_id, column_id):

    # get row based on given `row_key`, `column_family_id`, & `column_id`
    row = table.read_row(row_key)
    column_id_encoded = column_id.encode("utf-8")
    cols = row.cells[column_family_id][column_id_encoded]
    
    # loop over `values` in given `column_id`
    values = []
    for col in cols:
        temp_val = col.value.decode("utf-8")
        values.append(temp_val)

    print(column_id, values)


# setup credential gcp
setup_creds()

# setup bigtable
table = bigtable_config()

# define data to insert
row_key = 'row#key#1'
column_family_id = 'cf_hobbies'
person_hobbies_1 = [
    {'person': 'mike', 'hobby': 'swimming'},
    {'person': 'jhon', 'hobby': 'dancing'},
    {'person': 'melia', 'hobby': 'cooking'}
]
dataset = [
    {'person': 'melia', 'age': 23, 'hobbies': ['cooking']},
    {'person': 'mike', 'age': 22, 'hobbies': ['swimming', 'coding']},
    {'person': 'sundul', 'age': 20, 'hobbies': ['sleeping', 'read comics']},
    {'person': 'jane', 'age': 25, 'hobbies': ['gossip']},
    {'person': 'edward', 'age': 23, 'hobbies': ['gaming', 'sky diving', 'basketball']}
]

write_simple(table, row_key, column_family_id, dataset)
# read_columns_per_row_key(table, row_key, column_family_id)
# read_cell_by_column_id(table, row_key, column_family_id, 'mike')
