from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
import os
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import pendulum



@dag(
              dag_id="orchestrate",
              schedule="0 11 * * *",  # Run daily at 11
              catchup=False,
              start_date=pendulum.datetime(year=2026, month=6, day=18, tz="Canada/Eastern")
)
def orchestrate():

    @task
    def ingest_cdc():     

                ws = WorkspaceClient(
                host=os.environ["DATABRICKS_HOST"],
                token=os.environ["DATABRICKS_TOKEN"]
                )

                job_trigger = ws.jobs.run_now(job_id=964818441363594)


                while True:
                        job_run = ws.jobs.get_run(job_trigger.run_id)

                        print(f"Job run status: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")

                        if job_run.state.life_cycle_state in [
                                RunLifeCycleState.TERMINATED,
                                RunLifeCycleState.SKIPPED,
                                RunLifeCycleState.INTERNAL_ERROR,
                        ]:
                                if job_run.state.result_state == RunResultState.SUCCESS:
                                        print("Job completed successfully.")
                                        break
                                else:
                                        raise Exception(f"Job failed with state: {job_run.state.result_state}")

                        time.sleep(5)  # Wait for 10 seconds before checking the job status again

                return "CDC ingestion completed successfully."
          

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
            bash_command='cd /opt/airflow/walmartmain_project && dbt snapshot'
    )

    gold_facts = BashOperator(
            task_id='gold_facts',
            bash_command='cd /opt/airflow/walmartmain_project && dbt run --select gold/fact'
    )        

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()

