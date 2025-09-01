@echo off
REM Set Python path for Spark
set PYSPARK_PYTHON=C:\Users\jp\AppData\Local\Programs\Python\Python313\python.exe

REM Run the PySpark ETL script
spark-submit src\ingest.py
