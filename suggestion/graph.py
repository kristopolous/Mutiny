from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

_driver = None

def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver

def run_query(query, params=None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]

def get_total_artists():
    query = """
    MATCH (a:Artist)
    RETURN count(a) as total
    """
    result = run_query(query)
    return result[0]["total"] if result else 0

def get_artists_sharing_property(prop_type, prop_id):
    if prop_type == "label":
        query = """
        MATCH (a:Artist)-[:RELEASED_ON]->(l:Label {discogs_id: $prop_id})
        RETURN count(DISTINCT a) as count
        """
    elif prop_type == "producer":
        query = """
        MATCH (a:Artist)-[:PRODUCED_BY]->(p:Producer {discogs_id: $prop_id})
        RETURN count(DISTINCT a) as count
        """
    else:
        return 0
    
    result = run_query(query, {"prop_id": prop_id})
    return result[0]["count"] if result else 0