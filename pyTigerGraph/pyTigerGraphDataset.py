"""Data Ingestion Functions

Ingest stock datasets into a TigerGraph database.
All functions in this module are called as methods on a link:https://docs.tigergraph.com/pytigergraph/current/core-functions/base[`TigerGraphConnection` object]. 
"""
import logging

from .datasets import Datasets
from .pyTigerGraphAuth import pyTigerGraphAuth

logger = logging.getLogger(__name__)


class pyTigerGraphDataset(pyTigerGraphAuth):
    def ingestDataset(
        self,
        dataset: Datasets,
        cleanup: bool = True,
        getToken: bool = False
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
        logger.info("entry: ingestDataset")
        if logger.level == logging.DEBUG:
            logger.debug("params: " + self._locals(locals()))

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
                "Skip ingestion.".format(dataset.name)
            )
            print("Graph name is set to {} for this connection.".format(dataset.name))
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

        for resp in dataset.run_load_job(self):
            stats = resp[0]["statistics"]
            if "vertex" in stats:
                for vstats in stats["vertex"]:
                    print(
                        "Ingested {} objects into VERTEX {}".format(
                            vstats["validObject"], vstats["typeName"]
                        ),
                        flush=True,
                    )
            if "edge" in stats:
                for estats in stats["edge"]:
                    print(
                        "Ingested {} objects into EDGE {}".format(
                            estats["validObject"], estats["typeName"]
                        ),
                        flush=True,
                    )
            if logger.level == logging.DEBUG:
                logger.debug(str(resp))

        if cleanup:
            print("---- Cleaning ----", flush=True)
            dataset.clean_up()

        print("---- Finished ingestion ----", flush=True)
        logger.info("exit: ingestDataset")

    def check_exist_graphs(self, name: str) -> bool:
        "NO DOC"
        resp = self.gsql("ls")
        return "Graph {}".format(name) in resp
