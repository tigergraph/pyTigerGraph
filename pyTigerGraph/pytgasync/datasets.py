"""Datasets

Stock datasets that can be ingested into a TigerGraph database through the `ingestDataset`
function in pyTigerGraph.
"""
import json
import tarfile
import warnings
from abc import ABC, abstractmethod
from os import makedirs
from os.path import isdir
from os.path import join as pjoin
from shutil import rmtree
from urllib.parse import urljoin
from typing_extensions import Self
import io

import httpx

from pyTigerGraph.datasets import Datasets, BaseDataset


class AsyncDatasets(Datasets):
    # using a factory method instead of __init__ since we need to call asynchronous methods when creating this.
    # AsyncDatasets can only be created by calling: dataset = await AsyncDatasets.create()

    @classmethod
    async def create(cls, name: str = None, tmp_dir: str = "./tmp") -> Self:
        """Stock datasets.

        Please see https://tigergraph-public-data.s3.us-west-1.amazonaws.com/inventory.json[this link]
        for datasets that are currently available. The files for the dataset with `name` will be
        downloaded to local `tmp_dir` automatically when this class is instantiated. 
        For offline environments, download the desired .tar manually from the inventory page, and extract in the desired location.
        Specify the `tmp_dir` parameter to point to where the unzipped directory resides.


        Args:
            name (str, optional):
                Name of the dataset to get. If not provided or None, available datasets will be printed out.
                Defaults to None.
            tmp_dir (str, optional):
                Where to store the artifacts of this dataset. Defaults to "./tmp".
        """
        self = cls()
        BaseDataset.__init__(self, name)
        self.base_url = "https://tigergraph-public-data.s3.us-west-1.amazonaws.com/"
        self.tmp_dir = tmp_dir

        if not name:
            await self.list()
            return self

        # Download the dataset and extract
        if isdir(pjoin(tmp_dir, name)):
            print(
                "A folder with name {} already exists in {}. Skip downloading.".format(
                    name, tmp_dir
                )
            )

        if not isdir(pjoin(tmp_dir, name)):
            dataset_url = await self.get_dataset_url()
            # Check if it is an in-stock dataset.
            if not dataset_url:
                raise Exception("Cannot find this dataset in the inventory.")
            self.dataset_url = dataset_url
            self.download_extract()

        self.ingest_ready = True

        return self

    # For overriding Dataset's init method
    def __init__(self):
        pass

    async def get_dataset_url(self) -> str:
        "NO DOC"
        inventory_url = urljoin(self.base_url, "inventory.json")
        async with httpx.AsyncClient() as client:
            resp = await client.request("GET", inventory_url)
        resp.raise_for_status()
        resp = resp.json()
        if self.name in resp:
            return resp[self.name]
        else:
            return None

    async def download_extract(self) -> None:
        "NO DOC"
        makedirs(self.tmp_dir, exist_ok=True)
        client = httpx.AsyncClient()
        async with client.stream("GET", self.dataset_url) as resp:
            raw_content = b''
            async for byte in resp.aiter_raw():
                raw_content += byte
            raw_content = io.BytesIO(raw_content)
            try:
                from tqdm.auto import tqdm
                total_length = int(resp.headers.get("Content-Length"))
                with tqdm.wrapattr(
                    raw_content, "read", total=total_length, desc="Downloading"
                ) as raw:
                    with tarfile.open(fileobj=raw, mode="r|gz") as tarobj:
                        tarobj.extractall(path=self.tmp_dir)
            except ImportError:
                warnings.warn(
                    "Cannot import tqdm. Downloading without progress report.")
                with tarfile.open(fileobj=raw_content, mode="r|gz") as tarobj:
                    tarobj.extractall(path=self.tmp_dir)
                print("Dataset downloaded.")

    async def create_graph(self, conn) -> str:
        "NO DOC"
        with open(pjoin(self.tmp_dir, self.name, "create_graph.gsql"), "r") as infile:
            resp = await conn.gsql(infile.read())
        return resp

    async def create_schema(self, conn) -> str:
        "NO DOC"
        with open(pjoin(self.tmp_dir, self.name, "create_schema.gsql"), "r") as infile:
            resp = await conn.gsql(infile.read())
        return resp

    async def create_load_job(self, conn) -> None:
        "NO DOC"
        with open(
            pjoin(self.tmp_dir, self.name, "create_load_job.gsql"), "r"
        ) as infile:
            resp = await conn.gsql(infile.read())
        return resp

    async def run_load_job(self, conn) -> dict:
        "NO DOC"
        with open(pjoin(self.tmp_dir, self.name, "run_load_job.json"), "r") as infile:
            jobs = json.load(infile)

        for job in jobs:
            resp = await conn.runLoadingJobWithFile(
                pjoin(self.tmp_dir, self.name, job["filePath"]),
                job["fileTag"],
                job["jobName"],
                sep=job.get("sep", ","),
                eol=job.get("eol", "\n"),
                timeout=job.get("timeout", 60000),
                sizeLimit=job.get("sizeLimit", 128000000),
            )
            yield resp

    async def list(self) -> None:
        """List available stock datasets
        """
        inventory_url = urljoin(self.base_url, "inventory.json")
        async with httpx.AsyncClient() as client:
            resp = await client.request("GET", inventory_url)
        resp.raise_for_status()
        print("Available datasets:")
        for k in resp.json():
            print("- {}".format(k))
