from airflow.sdk import dag, task

@dag(
    "xcoms_dag"
)
def xcoms_dag():
    @task.python
    def first_task():
        print("fetching data from first task")
        fetched_data = {"data": [1, 2, 3]}
        return fetched_data
    
    @task.python
    def second_task(data: dict):
        fetched_data = data['data']
        transformed_data = [x * 2 for x in fetched_data]
        transformed_data_dict = {"transformed_data": transformed_data}
        return transformed_data_dict
    
    @task.python
    def third_task(data: dict):
        load_data = data
        return load_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

xcoms_dag()