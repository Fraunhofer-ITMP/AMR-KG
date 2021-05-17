from neo4j import GraphDatabase

"""
This is similar to the db-connection.py script.
This script does not create a new DB, it is used to populate the default
database at start up time.
"""

class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri,
                auth=(
                    self.__user,
                    self.__pwd
                )
            )
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(
                database=db
            ) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


def main():
    conn = Neo4jConnection(
        uri="bolt://localhost:7687",
        user="neo4j",  # Change according to user
        pwd="neo4jbinder"  # Change according to user
    )

    # Create person nodes
    create_person = """
        LOAD CSV WITH HEADERS FROM 'file:///data/person.csv' AS row
        CREATE (p:Person {id: toInteger(row.ID), 
        name: row.Contact, email: row.email,
        orcid: 'https://orcid.org/0000-0001-7655-2459'})
        """
    conn.query(create_person)

    # Creates institute nodes
    create_institute = """
    LOAD CSV WITH HEADERS FROM 'file:///data/institutes.csv' AS row
    CREATE (i:Institute {id: toInteger(row.ID),
    name: row.Institute})
    """
    conn.query(create_institute)

    # Create project nodes
    create_project = """
    LOAD CSV WITH HEADERS FROM 'file:///data/competences.csv' AS row
    CREATE (p:Project {id: toInteger(row.ID),
    name: row.Project})
    """
    conn.query(create_project)

    # Add Project -> Institute edges
    for i in range(1, 4):
        add_institute_data = "LOAD CSV WITH HEADERS FROM 'file:///data/competences.csv' AS row " + \
                             f"WITH row where row.Focus{i} is not null " + \
                             "MATCH (p:Project {id: toInteger(row.ID)})," + \
                             "(i:Institute {id: toInteger(" + f"row.Focus{i})" + "}) " + \
                             "CREATE (i)-[w:PROJECT {name:" + f"'Focus{i}'" + "}]->(p)"
        conn.query(add_institute_data)

    # Add Institute -> Person data
    add_person = """
    LOAD CSV WITH HEADERS FROM 'file:///data/group.csv' AS row
    MATCH (p:Person {id: toInteger(row.personID)}),
    (d:Project {id: toInteger(row.projectID)})
    CREATE (d)-[:PEOPLE]->(p)
    """
    conn.query(add_person)

    # Add person meta data such as skills and pathogens

    create_skills = """
    LOAD CSV WITH HEADERS FROM 'file:///data/skills.csv' AS row
    FIELDTERMINATOR ';'
    CREATE (s:Skill {id: toInteger(row.ID),
    skill: row.Skill})
    """
    conn.query(create_skills)

    create_pathogens = """
    LOAD CSV WITH HEADERS FROM 'file:///data/pathogens.csv' AS row
    CREATE (b:Bacteria {id: toInteger(row.ID),
    pathogen: row.Pathogens})
    """
    conn.query(create_pathogens)

    # Person -> Skill
    for i in range(1, 7):
        add_skill_data = "LOAD CSV WITH HEADERS FROM 'file:///data/person.csv' AS row " + \
                         f"WITH row where row.Skill_{i} is not null " + \
                         "MATCH (p:Person {id: toInteger(row.ID)})," + \
                         "(s:Skill {id: toInteger(" + f"row.Skill_{i})" + "})" + \
                         "CREATE (p)-[:SKILLS]->(s)"
        conn.query(add_skill_data)

    # Person -> Pathogen
    for i in range(1, 20):
        add_bacterial_data = "LOAD CSV WITH HEADERS FROM 'file:///data/person.csv' AS row " + \
                             f"WITH row where row.Pathogen_{i} is not null " + \
                             "MATCH (p:Person {id: toInteger(row.ID)})," + \
                             "(b:Bacteria {id: toInteger(" + f"row.Pathogen_{i})" + "})" + \
                             "CREATE (p)-[:PATHOGEN {name: 'Specializes_in'}]->(b)"

        conn.query(add_bacterial_data)


if __name__ == '__main__':
    main()
