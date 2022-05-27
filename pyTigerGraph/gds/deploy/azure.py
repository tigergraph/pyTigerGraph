from azureml.core import Workspace
from azureml.core.model import Model
from azureml.core import Environment
from azureml.core.model import InferenceConfig
from azureml.core.environment import CondaDependencies
from azureml.core.webservice import AciWebservice
from azureml.core.webservice import LocalWebservice
import tempfile
from .deploy import Deploy
import json
import os

class AzureML(Deploy):
    def __init__(self, conn: "TigerGraphConnection") -> None:
        self.conn = conn

    def __call__(self,
                model: "torch.nn.Module",
                model_definition_path: str,
                model_parameters: dict,
                workspace_name:str,
                model_name:str,
                environment_name:str, 
                service_name:str,
                subscription_id:str,
                resource_group:str,
                device:str = "cpu"):
        self.conn = self.conn

        parameters = model_parameters
        parameters["connection_config"] = self.conn.connection_config

        self.tempdir = tempfile.mkdtemp(prefix="azureml-"+model_name+"-")
        
        try:
            self._createModelDirectory(model = model, 
                                       parameters = parameters, 
                                       model_definition_path = model_definition_path, 
                                       model_directory_path = self.tempdir, 
                                       cloud_platform="azure")
        except:
            raise

        self.ws = Workspace(subscription_id=subscription_id,
                resource_group=resource_group,
                workspace_name=workspace_name)

        self.model = Model.register(self.ws, 
                        model_name = model_name,
                        model_path = self.tempdir+"/model.pth")

        self.service_name = service_name
        self.environment_name = environment_name
        self.device = device

        self.inf_config = self._createEnvironment()
        return self

    def _createEnvironment(self) -> InferenceConfig:
        env = Environment(name=self.environment_name)

        configs = json.load(open(self.tempdir+"/config.json"))
        out_form = configs["infer_loader_config"]["output_format"]

        conda_dep = CondaDependencies()
        conda_dep.add_pip_package("pyTigerGraph")
        
        if self.device.lower() == "cpu":
            conda_dep.add_channel("pytorch")
            conda_dep.add_conda_package("cpuonly")
        if out_form.lower() == "pyg":
            conda_dep.add_channel("pyg")
            conda_dep.add_conda_package("pyg")
        elif out_form.lower() == "dgl":
            conda_dep.add_channel("dgl")
            conda_dep.add_conda_package("dgl")

        # if requirements.txt exists, add it to the environment
        if os.path.exists(self.tempdir+"/requirements.txt"):
            with open(self.tempdir+"/requirements.txt", 'r') as f:
                for line in f:
                    conda_dep.add_pip_package(line.strip())

        env.environment_variables = {"AZUREML_SOURCE_DIR": self.tempdir}
        env.python.conda_dependencies = conda_dep

        return InferenceConfig(environment = env,
                               source_directory = self.tempdir,
                               entry_script="./score.py")

    def deployLocalModel(self) -> LocalWebservice:
        deployment_config = LocalWebservice.deploy_configuration(port=6789)

        service = Model.deploy(
            self.ws,
            self.service_name,
            [self.model],
            self.inf_config,
            deployment_config,
            overwrite=True,
        )
        return service

    def deployACIModel(self, 
                       cpu_cores:int = 2, 
                       memory_gb:int = 8, 
                       enable_authentication:bool = True) -> AciWebservice:

        deployment_config = AciWebservice.deploy_configuration(
            cpu_cores=cpu_cores, memory_gb=memory_gb, auth_enabled=enable_authentication
        )

        service = Model.deploy(
            self.ws,
            self.service_name,
            [self.model],
            self.inf_config,
            deployment_config,
            overwrite=True
        )
        return service