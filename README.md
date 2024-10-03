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