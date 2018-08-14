# Home Credit Default Risk Challenge Project


Table of Contents:
* [Introduction](#intro)
* [Installation](#installation)

## <a id='intro'></a> Introduction
------------

This repository explores [kaggle's Home Credit Default Risk](https://www.kaggle.com/c/home-credit-default-risk#description) 
challenge data.

## <a id='installation'></a> Installation
-------------

To set up a conda environment meeting all the necessary requirements to run the notebooks in this repository, run:

`conda create -n homeloans --file spec-file.txt`

To build the database, adjust `homeloans\config.py` accordingly (the data has to be unzipped to the `DATA_DIR`) and run
`python populate.py`.
