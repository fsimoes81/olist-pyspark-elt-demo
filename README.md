# olist-pyspark-elt-demo

A simple project using Pyspark to transform data.

This project uses Pyspark and Delta Tables to load csv files and transform them into delta table data lake.

I used [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). You must download it and extract the csv files into the /data/stage folder.

Use [Poetry](https://python-poetry.org/) to configure the project.

```bash
poetry lock
```
```bash
poetry install
```

In order to run the pipeline the project has a [Papermill](https://github.com/nteract/papermill) workflow.

Goes to:
```bash
cd src/utils
```

Run the code below in order to execute all three layers. You can run any layer add or removing parameters: brz, slv, gld.
```bash
python orchestration.py brz slv gld
```