
import requests


def sample_dag_carol():
    print("This is a sample DAG for Carol.")

    @task()
    def obter_usuarios():
        res = requests.get("https://jsonplaceholder.typicode.com/users").json()
        return res