import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('library')

with open('C:/BDS2/data/livre_data.json', 'r') as f:
    data = json.load(f)

for row in data:
    query = """
    INSERT INTO livre (id, titre, description, pays, categorie, auteur, editeur)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (row['_id'], row['Titre'], row['Description'],
                    row['Pays'], row['Categorie'], row['Auteur'], row['Editeur']))

cluster.shutdown()
