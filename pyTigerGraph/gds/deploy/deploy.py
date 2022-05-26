import os
import json
import torch

class TigerGraphCloudDeploymentException(Exception):
    def __init__(self, message):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

class Deploy(object):
    def __init__(self):
        self.toAzureML = None

    def __getattribute__(self, name):
        if name == "toAzureML":
            if super().__getattribute__(name) is None:
                try:
                    from .azure import toAzureML
                    self.toAzureML = toAzureML
                    return super().__getattribute__(name)
                except:
                    raise TigerGraphCloudDeploymentException("Please install the required AzureML packages. "
                                    "Run `pip install azureml-core` to install the required packages.")
            else:
                return super().__getattribute__(name)
        else:
            return super().__getattribute__(name)

    def createModelDirectory(self,
                             model:torch.nn.Module,
                             parameters:dict,
                             model_definition_path:str,
                             model_directory_path:str,
                             cloud_platform="azure") -> None:

        # Create the model directory
        if not os.path.exists(model_directory_path):
            os.makedirs(model_directory_path)
        
        # Copy the model definition file to the model directory
        if not os.path.exists(model_directory_path+"/model.py"):
            with open(model_definition_path, 'r') as f:
                model_str = f.read()
            with open(model_directory_path+"/model.py", 'w') as f:
                f.write(model_str)

        # Write the parameters to the model directory
        if not os.path.exists(model_directory_path+"/config.json"):
            with open(model_directory_path+"/config.json", 'w') as f:
                json.dump(parameters, f)

        # Save model weights
        if not os.path.exists(model_directory_path+"/model.pth"):
            torch.save(model.state_dict(), model_directory_path+"/model.pth")

        # Write inference script to the model directory
        if cloud_platform.lower() == "azure":
            script_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "inference_scripts",
                "azure.txt",
            )
            with open(model_directory_path+"/score.py", 'w') as f:
                with open(script_path, 'r') as script:
                    f.write(script.read())

            return model_directory_path
        else:
            raise TigerGraphCloudDeploymentException("Cloud platform not supported.")