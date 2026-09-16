from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag
def orchestrate():

    @task
    def ingest_cdc():
        return "CDC data ingested"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmartmain_project/target && rm -rf /opt/airflow/walmartmain_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running the dbt command
        return "cd /opt/airflow/walmartmain_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id='silver_technical',
        bash_command='cd /opt/airflow/walmartmain_project && dbt run --select silver_t' 
    )

    silver_technical_tests = BashOperator(
            task_id='silver_technical_tests',
            bash_command='cd /opt/airflow/walmartmain_project && dbt test --select silver_t' 
    )

    silver_business = BashOperator(
            task_id='silver_business',
            bash_command='cd /opt/airflow/walmartmain_project && dbt run --select silver_b' 
    )

    silver_business_tests = BashOperator(
            task_id='silver_business_tests',
            bash_command='cd /opt/airflow/walmartmain_project && dbt test --select silver_b' 
    )

    gold_ephemeral = BashOperator(
            task_id='gold_ephemeral',
            bash_command='cd /opt/airflow/walmartmain_project && dbt run --select gold/ephemeral' 
    )

    gold_dimensions = BashOperator(
            task_id='gold_dimensions',
            bash_command='cd /opt/airflow/walmartmain_project && dbt snapshots'
    )

    gold_facts = BashOperator(
            task_id='gold_facts',
            bash_command='cd /opt/airflow/walmartmain_project && dbt run --select gold/fact'
    )        

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()

