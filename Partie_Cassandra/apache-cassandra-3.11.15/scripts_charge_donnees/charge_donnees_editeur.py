import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('library')

with open('C:/BDS2/data/editeur_data.json', 'r') as f:
    data = json.load(f)

for row in data:
    query = """
    INSERT INTO editeur (id, prenom, nom, genre, date_naissance)
    VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query, (row['_id'], row['Prenom'], row['Nom'],
                    row['Genre'], row['date_naissance']))

cluster.shutdown()
