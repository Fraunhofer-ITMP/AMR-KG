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
This knowledge graph (KG) is an attempt to capture antimicrobial strain related MIC data with help of publically availble resources such as PubChem, ChEMBL, and SPARK data. It
makes use of FAIR (Findable, Accessible, Interpretable, and Reproducibite) standard for representing data.

**To visualize the knowledge graph, please click on the binder icon above (first time you do it it will take some minutes, be patient).**
</p>



## Ontolgoy overview

For the KG, we make use of the following ontologies:
1. **[NCBI](https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi)** - To normalize the species or organisms
2. **[ChEMBL](https://www.ebi.ac.uk/chembl/)** - To normalize the chemicals as well as bioassay reports  
3. **[PubChem](https://pubchem.ncbi.nlm.nih.gov/)** - To normalize chemiclas not found in ChEMBL
4. **[SPARK](https://www.collaborativedrug.com/public-access/)** - To normalize chemicals not found in the above two databases
5. **[ORCID](https://orcid.org/)** - To associate each individual with an CV. (In this test case, all the individuals are dummy names and have been associated with single ORCID ID.)

## How to run the KG?

1. Once the binder is loaded, open the Firefox browser
2. In the browser, type localhost:7474
3. Then enter the following username and password: 
    1. Username = neo4j
    2. Password = neo4jbinder 

You are now looged into the KG. Feel free to browse and play with it!


## Partners
<p align="center">
    <img src="https://www.imi.europa.eu/sites/default/files/styles/facebook/public/projects/logos/IMI%20AMR%20Accelerator_logo.jpg?itok=ghj1Z1T0" width="400">
</p>
