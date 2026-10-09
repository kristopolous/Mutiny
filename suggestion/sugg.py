import sqlite3
from collections import Counter
import json

# Setup database
conn = sqlite3.connect('music_rec.db')
c = conn.cursor()

# Create tables
c.execute('''CREATE TABLE IF NOT EXISTS ratings (
    album_uuid TEXT PRIMARY KEY,
    rating INTEGER
)''')

c.execute('''CREATE TABLE IF NOT EXISTS album_entities (
    album_uuid TEXT,
    entity_uuid TEXT,
    entity_type TEXT,
    PRIMARY KEY (album_uuid, entity_uuid)
)''')

c.execute('''CREATE TABLE IF NOT EXISTS entity_weights (
    entity_uuid TEXT PRIMARY KEY,
    weight REAL
)''')

conn.commit()

# Function to calculate entity weights (run once after loading data)
def calculate_weights():
    c.execute('SELECT entity_uuid, COUNT(*) as freq FROM album_entities GROUP BY entity_uuid')
    weights = {}
    for entity_uuid, freq in c.fetchall():
        weights[entity_uuid] = 1.0 / (1 + freq)  # Inverse frequency
    
    for entity_uuid, weight in weights.items():
        c.execute('INSERT OR REPLACE INTO entity_weights VALUES (?, ?)', (entity_uuid, weight))
    conn.commit()

# Function to get recommendations
def get_recommendations(limit=50):
    query = '''
        SELECT ae2.album_uuid, 
               SUM(r.rating * ew.weight) as score
        FROM ratings r
        JOIN album_entities ae1 ON r.album_uuid = ae1.album_uuid
        JOIN entity_weights ew ON ae1.entity_uuid = ew.entity_uuid
        JOIN album_entities ae2 ON ae1.entity_uuid = ae2.entity_uuid
        WHERE ae2.album_uuid NOT IN (SELECT album_uuid FROM ratings)
        GROUP BY ae2.album_uuid
        ORDER BY score DESC
        LIMIT ?
    '''
    c.execute(query, (limit,))
    return c.fetchall()

# Example usage:
# 1. Load your discogs data into album_entities
# 2. Load user ratings into ratings table
# 3. calculate_weights()
# 4. recommendations = get_recommendations()

conn.close()
