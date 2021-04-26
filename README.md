# Neo4J-graph
Repository for Neo4J graph database

1. data - data folder containing all the data files for the neo4j database
1. db-connection.py - Python script using Neo4J for connecting to local DB and storing data in it.

To run the notebooks or the scripts, you can either build locally with [repo2docker](https://repo2docker.readthedocs.io/) or [run on mybinder.org](https://mybinder.org/v2/gh/ITeMP-temp/Neo4J-graph/main?filepath=notebooks)

To build locally:

 * Install [Docker](https://www.docker.com/) if required
 * Create a virtual environment and install repo2docker from PyPI.
 * Clone this repository
 * Run ``repo2docker``. 
 * Depending on the permissions, you might have to run the command as an admin


```
pip install jupyter-repo2docker
git clone https://github.com/ITeMP-temp/Neo4J-graph.git
cd Neo4J-graph
repo2docker .
```
