import person_hobbies_pb2

person_hobbies = person_hobbies_pb2.PersonHobbies()

dataset = [
    {'person': 'melia', 'age': 23, 'hobbies': ['cooking']},
    {'person': 'mike', 'age': 22, 'hobbies': ['swimming', 'coding']},
    {'person': 'sundul', 'age': 20, 'hobbies': ['sleeping', 'read comics']},
    {'person': 'jane', 'age': 25, 'hobbies': ['gossip']},
    {'person': 'edward', 'age': 23, 'hobbies': ['gaming', 'sky diving', 'basketball']}
]

for data in dataset:
    
    person = person_hobbies_pb2.Person()
    person.name = data['person']
    person.age = data['age']
    
    for hobby in data['hobbies']:
        person.hobbies.add(name=hobby)

    person_hobbies.person.append(person)

print(person_hobbies)