# Airflow-sample
Using Python Operator in Apache Airflow

Apache Airflow is an orchestrating tool to create, schedule and monitor workflows.
It comes with many plug and play Operators which makes it easy to execute workflows
present on most of the infrastructures and Cloud services.

In this example code I have created a dag which executes a Bash command and invokes a python function.
This function fetches records from Airflow DB and calls REST API request.
The Airflow DB connection details can be configured from the UI.

This Python function is just an example of task performed and could contain any workflow logic 
including ETL jobs.
