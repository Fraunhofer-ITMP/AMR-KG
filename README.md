# Jupyter Desktop
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ITeMP-temp/Neo4J-graph/master?urlpath=desktop)
# Neo4J-graph
Repository for Neo4J graph database

1. data - data folder containing all the data files for the neo4j database
1. connection.py - Python script using Neo4J for connecting to local DB and storing data in it.


To run the database, you can either build locally with [repo2docker](https://repo2docker.readthedocs.io/) or [run on mybinder.org](https://mybinder.org/v2/gh/ITeMP-temp/Neo4J-graph/master?urlpath=desktop)

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

Run XFCE (or other desktop environments) on Jupyter.
When this extension is launched it will run a Linux desktop on the Jupyter single-user server, and proxy it to your browser using VNC via Jupyter.
Neo4j community is also installed and can be accessed via the web UI after launching the Desktop.
To open it,
 
* Launch the Desktop
* Open Firefox
* Enter localhost:7474
