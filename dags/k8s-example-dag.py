from airflow.decorators import dag, task
from datetime import datetime

import os
import json
import requests
from kubernetes.client import models as k8s

new_config ={ "pod_override": k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(labels={"purpose": "pod-override-example"}),
                spec=k8s.V1PodSpec(
                    volumes=[
                        k8s.V1Volume(name="dags", empty_dir=k8s.V1EmptyDirVolumeSource())
                    ],
                    containers=[
                        k8s.V1Container(
                            name="base",
                            env=[
                                k8s.V1EnvVar(name="STATE", value="wa"),
                                k8s.V1EnvVar(name="AIRFLOW__CORE__DAGS_FOLDER", value="/usr/local/airflow/dags/latest/dags")
                               ],
                            volume_mounts=[
                                k8s.V1VolumeMount(name="dags", mount_path="/usr/local/airflow/dags")
                                ]
                            ),
                        k8s.V1Container(
                            name="git-sync",
                            image="k8s.gcr.io/git-sync/git-sync:v3.3.0",
                            image_pull_policy="IfNotPresent",
                            # env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="gitsync"))],
                            env=[ # Optionally the following env variables can be added to a `gitsync` secret and the above env_from can be used instead.
                                k8s.V1EnvVar(name="GIT_SYNC_REPO", value="https://github.com/astronomer/gitsync-example.git"),
                                k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value="main"),
                                k8s.V1EnvVar(name="GIT_SYNC_ROOT", value="/usr/local/airflow/dags"),
                                k8s.V1EnvVar(name="GIT_SYNC_DEST", value="latest"),
                                k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true")
                                ],
                            volume_mounts=[
                                k8s.V1VolumeMount(name="dags", mount_path="/usr/local/airflow/dags", read_only=False)
                                ]
                            )
                        ]
                    )
                )
            }
            

default_args = {
    'start_date': datetime(2021, 1, 1)
}

@dag('k8s_executor_example', schedule_interval='@daily', default_args=default_args, catchup=False)
def taskflow():

    @task(executor_config=new_config)
    def get_testing_increase():
        """
        Gets totalTestResultsIncrease field from Covid API for given state and returns value
        """
        url = 'https://covidtracking.com/api/v1/states/'
        res = requests.get(url+'{0}/current.json'.format(os.environ['STATE']))
        print(res.text)
        return{'testing_increase': json.loads(res.text)['totalTestResultsIncrease']}

    get_testing_increase()

dag = taskflow()
