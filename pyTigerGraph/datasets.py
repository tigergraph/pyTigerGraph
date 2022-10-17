"""datasets

In-stock datasets that can be ingested into a TigerGraph database through the `ingestDataset`
function in pyTigerGraph.
"""
import json
import tarfile
from abc import ABC, abstractmethod
from os import makedirs
from os.path import join as pjoin
from shutil import rmtree
from urllib.parse import urljoin

import requests
from tqdm.auto import tqdm


class BaseDataset(ABC):
    def __init__(self, name: str = None) -> None:
        self.name = name
        self.ingest_ready = False

    @abstractmethod
    def create_graph(self, conn) -> str:
        pass

    @abstractmethod
    def create_schema(self, conn) -> str:
        pass

    @abstractmethod
    def create_load_job(self, conn) -> None:
        pass

    @abstractmethod
    def run_load_job(self, conn) -> None:
        pass


class Datasets(BaseDataset):
    def __init__(self, name: str = None, tmp_dir: str = "./tmp") -> None:
        """In-stock datasets.

        Please see "https://tigergraph-public-data.s3.us-west-1.amazonaws.com/inventory.json"
        for datasets that are currently available. The files for the dataset with `name` will be
        downloaded to local `tmp_dir` automatically when this class is instantiated.

        Args:
            name (str, optional): 
                Name of the dataset to get. Defaults to None.
            tmp_dir (str, optional): 
                Where to store the artifacts of this dataset. Defaults to "./tmp".
        """        
        super().__init__(name)
        self.base_url = "https://tigergraph-public-data.s3.us-west-1.amazonaws.com/"
        self.tmp_dir = tmp_dir

        if not name:
            return

        # Check if it is an in-stock dataset.
        dataset_url = self.get_dataset_url()
        if not dataset_url:
            raise Exception("Cannot find this dataset in the inventory.")
        self.dataset_url = dataset_url

        # Download the dataset and extract
        self.download_extract()

        self.ingest_ready = True

    def get_dataset_url(self) -> str:
        inventory_url = urljoin(self.base_url, "inventory.json")
        resp = requests.get(inventory_url)
        resp.raise_for_status()
        resp = resp.json()
        if self.name in resp:
            return resp[self.name]
        else:
            return None

    def download_extract(self) -> None:
        makedirs(self.tmp_dir, exist_ok=True)
        with requests.get(self.dataset_url, stream=True) as resp:
            total_length = int(resp.headers.get("Content-Length"))
            with tqdm.wrapattr(
                resp.raw, "read", total=total_length, desc="Downloading"
            ) as raw:
                with tarfile.open(fileobj=raw, mode="r|gz") as tarobj:
                    tarobj.extractall(path=self.tmp_dir)

    def clean_up(self) -> None:
        rmtree(pjoin(self.tmp_dir, self.name))
        self.ingest_ready = False

    def create_graph(self, conn) -> str:
        with open(pjoin(self.tmp_dir, self.name, "create_graph.gsql"), "r") as infile:
            resp = conn.gsql(infile.read())
        return resp

    def create_schema(self, conn) -> str:
        with open(pjoin(self.tmp_dir, self.name, "create_schema.gsql"), "r") as infile:
            resp = conn.gsql(infile.read())
        return resp

    def create_load_job(self, conn) -> None:
        with open(
            pjoin(self.tmp_dir, self.name, "create_load_job.gsql"), "r"
        ) as infile:
            resp = conn.gsql(infile.read())
        return resp

    def run_load_job(self, conn) -> None:
        with open(pjoin(self.tmp_dir, self.name, "run_load_job.json"), "r") as infile:
            jobs = json.load(infile)

        resp = []
        for job in jobs:
            resp.append(
                conn.runLoadingJobWithFile(
                    pjoin(self.tmp_dir, self.name, job["filePath"]),
                    job["fileTag"],
                    job["jobName"],
                    sep=job.get("sep", ","),
                    eol=job.get("eol", "\n"),
                    timeout=job.get("timeout", 60000),
                    sizeLimit=job.get("sizeLimit", 128000000)
                )
            )
        return resp