clean:
	rm -rf *.model delta_tests delta_table_metadata header.csv output.csv output.parquet single.csv spark_warehouse

readme:
	${PYSPARK_PYTHON} cheatsheet.py

notebook:
	${PYSPARK_PYTHON} cheatsheet.py --notebook

black:
	black cheatsheet.py

all: black readme notebook
