from azureml.core import Workspace
from azureml.core.model import Model
from azureml.core import Environment
from azureml.core.model import InferenceConfig
from azureml.core.environment import CondaDependencies
import json
import os

def toAzureML(model_directory:str,
              workspace_name:str,
              model_name:str,
              environment_name:str, 
              service_name:str,
              subscription_id:str,
              resource_group:str,
              device:str = "cpu",
              cpu_cores:int = 2,
              memory_gb:int = 8,
              enable_authentication:bool = True,
              local_deploy:bool = False):

    ws = Workspace(subscription_id=subscription_id,
               resource_group=resource_group,
               workspace_name=workspace_name)

    model = Model.register(ws, 
                           model_name = model_name,
                           model_path = model_directory+"/model.pth")
    
    env = Environment(name=environment_name)

    configs = json.load(open(model_directory+"/config.json"))
    out_form = configs["infer_loader_config"]["output_format"]

    conda_dep = CondaDependencies()
    conda_dep.add_pip_package("pyTigerGraph")
    
    if device.lower() == "cpu":
        conda_dep.add_channel("pytorch")
        conda_dep.add_conda_package("cpuonly")
    if out_form.lower() == "pyg":
        conda_dep.add_channel("pyg")
        conda_dep.add_conda_package("pyg")
    elif out_form.lower() == "dgl":
        conda_dep.add_channel("dgl")
        conda_dep.add_conda_package("dgl")

    # if requirements.txt exists, add it to the environment
    if os.path.exists(model_directory+"/requirements.txt"):
        with open(model_directory+"/requirements.txt", 'r') as f:
            for line in f:
                conda_dep.add_pip_package(line.strip())

    env.environment_variables = {"AZUREML_SOURCE_DIR": model_directory}
    env.python.conda_dependencies = conda_dep

    inf_config = InferenceConfig(environment = env,
                                 source_directory = model_directory,
                                 entry_script="./score.py")
    
    if local_deploy:
        from azureml.core.webservice import LocalWebservice
        deployment_config = LocalWebservice.deploy_configuration(port=6789)

        service = Model.deploy(
            ws,
            service_name,
            [model],
            inf_config,
            deployment_config,
            overwrite=True,
        )
    else:
        from azureml.core.webservice import AciWebservice

        deployment_config = AciWebservice.deploy_configuration(
            cpu_cores=cpu_cores, memory_gb=memory_gb, auth_enabled=enable_authentication
        )

        service = Model.deploy(
            ws,
            service_name,
            [model],
            inf_config,
            deployment_config,
            overwrite=True
        )
    return service
