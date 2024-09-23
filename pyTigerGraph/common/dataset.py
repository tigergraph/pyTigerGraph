import logging

from pyTigerGraph.datasets import Datasets

logger = logging.getLogger(__name__)

def _parse_ingest_dataset(responses: str, cleanup: bool, dataset: Datasets):
    for resp in responses:
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
