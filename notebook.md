Notebook How-To
===============

Overview
--------
Notebooks are a good way to iteratively explore and visualize data.

Code in [pyspark-cheatsheet](https://github.com/cartershanklin/pyspark-cheatsheet) can be run in any Spark notebook with little or no modification.

This how-to shows how to run code in a Jupyter notebook on a local Docker container. The same approach can be used on any Spark notebook.

Install Docker
--------------
Install Docker using their [installation instructions](https://docs.docker.com/get-docker/).


Install and Start a Jupyter Notebook
------------------------------------
```sh
docker pull jupyter/pyspark-notebook
docker run -it --rm -p 8888:8888 -p 4040:4040 -p 4041:4041 jupyter/pyspark-notebook
```

The second command will produce a link you need to click on to open the Jupyter notebook, for example you may see:
```
[C 20:48:13.082 NotebookApp]

    To access the notebook, open this file in a browser:
        file:///home/jovyan/.local/share/jupyter/runtime/nbserver-7-open.html
    Or copy and paste one of these URLs:
        http://21ea37496cd2:8888/?token=5713d34d0c7b3580d925e9dec04636319a982119e085aa40
     or http://127.0.0.1:8888/?token=5713d34d0c7b3580d925e9dec04636319a982119e085aa40
```

In this case you would need to navigate to the bottom link with a web browser.

Clone the Repository
--------------------
git clone https://github.com/cartershanklin/pyspark-cheatsheet


Add Delta Libraries
-------------------
pip3 install --user delta-spark


Open the Notebook
-----------------


Initialize Spark and Data
-------------------------


Run Cells
---------
