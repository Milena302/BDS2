import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('library')

with open('C:/BDS2/data/adherent.json', 'r') as f:
    data = json.load(f)

for row in data:
    query = """
    INSERT INTO adherent (id, prenom, nom, genre, date_inscription, email, telephone, adresse)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (row['_id'], row['prenom'], row['nom'],
                    row['genre'], row['date_inscription'], row['email'],
                    row['telephone'], row['adresse']))

cluster.shutdown()
