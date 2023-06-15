import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('library')

with open('C:/BDS2/data/categorie_data.json', 'r') as f:
    data = json.load(f)

for row in data:
    query = """
    INSERT INTO categorie (id, titre, description)
    VALUES (%s, %s, %s)
    """
    session.execute(query, (row['_id'], row['Titre'], row['Description']))

cluster.shutdown()
