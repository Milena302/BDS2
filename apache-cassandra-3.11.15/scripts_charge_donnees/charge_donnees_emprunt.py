import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('library')

with open('C:/BDS2/data/emprunt.json', 'r') as f:
    data = json.load(f)

for row in data:
    query = """
    INSERT INTO emprunt (id, date_emprunt, date_limite_retour, date_retour, id_adherent, id_employe, id_livre)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (row['_id'], row['date_emprunt'], row['date_limite_retour'],
                    row['date_retour'], row['id_adherent'], row['id_employe'], row['id_livre']))

cluster.shutdown()
