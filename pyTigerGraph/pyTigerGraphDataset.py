"""Data Ingestion Functions

Ingest stock datasets into a TigerGraph database.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""
from .datasets import Datasets
from .pyTigerGraphAuth import pyTigerGraphAuth


class pyTigerGraphDataset(pyTigerGraphAuth):
    def ingestDataset(
        self, dataset: Datasets, cleanup: bool = True, getToken: bool = False
    ) -> None:
        """Ingest a stock dataset to a TigerGraph database.

        Args:
            dataset (Datasets):
                A Datasets object as `pyTigerGraph.datasets.Datasets`.
            cleanup (bool, optional):
                Whether or not to remove local artifacts downloaded by `Datasets`
                after ingestion is done. Defaults to True.
            getToken (bool, optional):
                Whether or not to get auth token from the database. This is required
                when auth token is enabled for the database. Defaults to False.
        """
        if not dataset.ingest_ready:
            raise Exception("This dataset is not ingestable.")

        print("---- Checking database ----", flush=True)
        if self.check_exist_graphs(dataset.name):
            # self.gsql("USE GRAPH {}\nDROP JOB ALL\nDROP GRAPH {}".format(
            #     dataset.name, dataset.name
            # ))
            self.graphname = dataset.name
            if getToken:
                self.getToken(self.createSecret())
            print(
                "A graph with name {} already exists in the database. "
                "Please drop it first before ingesting.".format(dataset.name)
            )
            return

        print("---- Creating graph ----", flush=True)
        resp = dataset.create_graph(self)
        print(resp, flush=True)
        if "Failed" in resp:
            return

        print("---- Creating schema ----", flush=True)
        resp = dataset.create_schema(self)
        print(resp, flush=True)
        if "Failed" in resp:
            return

        print("---- Creating loading job ----", flush=True)
        resp = dataset.create_load_job(self)
        print(resp, flush=True)
        if "Failed" in resp:
            return

        print("---- Ingesting data ----", flush=True)
        self.graphname = dataset.name
        if getToken:
            self.getToken(self.createSecret())

        resp = dataset.run_load_job(self)
        print(resp, flush=True)

        if cleanup:
            print("---- Cleaning ----", flush=True)
            dataset.clean_up()

        print("---- Finished ingestion ----", flush=True)

    def check_exist_graphs(self, name: str) -> bool:
        "NO DOC"
        resp = self.gsql("ls")
        return "Graph {}".format(name) in resp
