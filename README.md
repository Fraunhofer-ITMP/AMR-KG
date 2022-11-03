<h1 align="center">
  <br>
    AMR Knowledge Graph
    <br>
   <a href="https://github.com/Fraunhofer-ITMP/AMR-KG/actions/workflows/repo2docker.yml">
    <img src="https://github.com/Fraunhofer-ITMP/SASC/workflows/repo2docker/badge.svg"
         alt="GitHub Actions">
  </a>
  <a href="https://mybinder.org/v2/gh/Fraunhofer-ITMP/AMR-KG/main?urlpath=desktop">
    <img src="https://mybinder.org/badge_logo.svg" alt="AMR-KG">
  </a>
  <br>
</h1>

<p align="center">
This knowledge graph (KG) is an attempt to capture antimicrobial strain related MIC data with help of publicly available resources such as PubChem, ChEMBL, and SPARK data. It
makes use of FAIR (Findable, Accessible, Interpretable, and Reproducible) standard for representing data.
</p>

> To visualize the knowledge graph, please click on the binder icon above (first time you do it, it will take some minutes, be patient).


## Terminologies and ontologies used

For the KG, we make use of the following ontologies:
1. **[NCBI](https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi)** - To represent the bacterial species
2. **[ChEMBL](https://www.ebi.ac.uk/chembl/)** - To represent the chemicals as well as bioassay reports extracted from ChEMBL
3. **[PubChem](https://pubchem.ncbi.nlm.nih.gov/)** - To represent chemicals found in PubChem only
4. **[SPARK](https://www.collaborativedrug.com/public-access/)** - To represent chemicals restricted to SPARK
5. **[ORCID](https://orcid.org/)** - To associate each individual with a professional CV. (In this test case, all the individuals are dummy names and have been associated with test ORCID ids.)

## How to run the KG?

To run the database, you can either build locally with [repo2docker](https://repo2docker.readthedocs.io/) or [run on mybinder.org](https://mybinder.org/v2/gh/ITeMP-temp/AMR-KG/main?urlpath=desktop)

> **NOTE: repo2docker does not work on Windows.**

#### Running the KG online

1. Once the binder is loaded, open the Firefox browser


2. In the browser, type localhost:7474


3. Then enter the following username and password: 
    1. Username = neo4j
    2. Password = neo4jbinder 

You are now logged into the KG. Feel free to browse and play with it!

#### Running the KG locally

To build locally:

 * Install [Docker](https://www.docker.com/) if required
 * Create a virtual environment and install repo2docker from PyPI.
 * Clone this repository
 * Run ``repo2docker``. 
 * Depending on the permissions, you might have to run the command as an admin

```shell
pip install jupyter-repo2docker
git clone https://github.com/ITeMP-temp/Neo4J-graph.git
cd Neo4J-graph
repo2docker .
```

Run XFCE (or other desktop environments) on Jupyter.
When this extension is launched it will run a Linux desktop on the Jupyter single-user server, and proxy it to your browser using VNC via Jupyter.
Neo4j Community version is also installed and can be accessed via the web UI after launching the Desktop.

To open it,
 
* Launch the Desktop
* Open Firefox
* Enter localhost:7474

## Partners

> **_NOTE:_** This is an open source version of an internal AMR-KG setup within [IMI AMR Accelerator](https://amr-accelerator.eu/) with controlled access to consortium members only.

<p align="center">
    <img src="https://www.imi.europa.eu/sites/default/files/styles/facebook/public/projects/logos/IMI%20AMR%20Accelerator_logo.jpg?itok=ghj1Z1T0" width="400">
</p>
