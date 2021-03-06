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


def person_hobbies_proto_object():

    return person_hobbies_pb2.PersonHobbies()


def person_proto_object():

    return person_hobbies_pb2.Person()


def write_data(table, row_key, column_family_id, dataset):

    person_hobbies = person_hobbies_proto_object()
    row = table.direct_row(row_key)

    for data in dataset:
    
        person = person_proto_object()
        person.name = data['person']
        person.age = data['age']
        
        for hobby in data['hobbies']:
            person.hobbies.add(name=hobby)

        person_hobbies.person.append(person)
        
    person_hobbies_in_bytes = person_hobbies.SerializeToString() # convert proto object to bytes
    row.set_cell(column_family_id, 'data_1', person_hobbies_in_bytes)
    row.commit()

    print("Successfully wrote row {}.".format(row_key))


def write_append(table, row_key, column_family_id, dataset):

    person_hobbies = person_hobbies_proto_object()
    row = table.row(row_key, append=True)

    for data in dataset:
    
        person = person_proto_object()
        person.name = data['person']
        person.age = data['age']
        
        for hobby in data['hobbies']:
            person.hobbies.add(name=hobby)

        person_hobbies.person.append(person)
        
    person_hobbies_in_bytes = person_hobbies.SerializeToString() # convert proto object to bytes
    row.append_cell_value(column_family_id, 'data_1', person_hobbies_in_bytes)
    row.commit()

    print("Successfully wrote row {}.".format(row_key))


def read_bytes(table, row_key, column_family_id):

    person_hobbies = person_hobbies_proto_object()

    # get row based on given `row_key` & `column_family_id`
    row = table.read_row(row_key)
    rows = row.cells[column_family_id]

    for key in rows.keys():
        values = rows[key]

        for value in values:
            person_hobbies.ParseFromString(value.value)

    print(person_hobbies)


# setup credential gcp
setup_creds()

# setup bigtable
table = bigtable_config()

# define data to insert
row_key = 'row#key#1'
column_family_id = 'cf_hobbies'
dataset = [
    {'person': 'melia', 'age': 23, 'hobbies': ['cooking']},
    {'person': 'mike', 'age': 22, 'hobbies': ['swimming', 'coding']},
    {'person': 'sundul', 'age': 20, 'hobbies': ['sleeping', 'read comics']},
    {'person': 'jane', 'age': 25, 'hobbies': ['gossip']},
    {'person': 'edward', 'age': 23, 'hobbies': ['gaming', 'sky diving', 'basketball']}
]
dataset_additional = [
    {'person': 'melia', 'age': 23, 'hobbies': ['cooking', 'make up']},
    {'person': 'snow', 'age': 24, 'hobbies': ['hacking']}
]

# write_data(table, row_key, column_family_id, dataset)
# write_append(table, row_key, column_family_id, dataset_additional)
read_bytes(table, row_key, column_family_id)