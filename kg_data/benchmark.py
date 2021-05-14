import orjson
import dask.bag as db
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from kg_data.spark import get_spark_context
from kg_data.misc import Timer
from dotenv import load_dotenv


if __name__ == '__main__':
    load_dotenv("/data/binhvu/workspace/sm-dev/.env")
    client = Client()

    indir = "/data/binhvu/workspace/sm-dev/data/wikidata/step_1/*.gz"

    # with Timer.get_instance().watch_and_report("Spark execution time"):
    #     sc = get_spark_context()
    #     print(sc.textFile(indir).map(orjson.loads).map(lambda x: x['id']).count())

    with Timer.get_instance().watch_and_report("Dash execution time"):
        # with ProgressBar():
        print(db.read_text(indir).map(orjson.loads).map(lambda x: x['id']).count().compute())
