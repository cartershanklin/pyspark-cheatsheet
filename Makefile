clean:
	rm -rf best_pipeline_model.model delta_tests fit_cross_validator.model header.csv output.csv output.parquet single.csv spark_warehouse

readme:
	${PYSPARK_PYTHON} cheatsheet.py

notebook:
	${PYSPARK_PYTHON} cheatsheet.py --notebook

black:
	black cheatsheet.py

all: black readme notebook
