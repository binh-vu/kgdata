import ujson, time, rdflib, os, subprocess, logging
from tqdm.auto import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import *
from datetime import datetime
from pathlib import Path


FUSEKI_DIR = os.environ.get('FUSEKI_DIR', "/workspace/sm-dev/data/cache/fuseki")


class Fuseki:
    instance = None
    logger = logging.getLogger("kg_data.fuseki")

    def __init__(self):
        self.container_name = 'jena-wikidata'
        # directory that contains the data
        self.data_dir = None
        # fixed values for port and admin_pwd for the fuseki server
        # if we want to vary this, look at name filter in docker ps command
        self.port = 3030
        self.admin_pwd = "sm-dev"
        self.db_name = None

    @staticmethod
    def get_instance():
        if Fuseki.instance is None:
            Fuseki.instance = Fuseki()
        return Fuseki.instance

    def start(self, data_dir: str) -> 'Fuseki':
        """Start Fuseki service"""
        logger.info(f"Going to use {FUSEKI_DIR} to store data if the fuseki service starts")

        self.data_dir = os.path.abspath(data_dir)
        if self.is_started():
            resp = subprocess.check_output("docker inspect %s --format='{{json .Mounts}}'" % self.container_name,
                                           shell=True)
            volumes = {
                map['Destination']: map['Source']
                for map in ujson.loads(resp.decode())
            }
            assert volumes['/fuseki'] == FUSEKI_DIR
            assert volumes['/data'] == self.data_dir
            print("Connected to a running fuseki service")
        else:
            print("Start a new fuseki service")
            subprocess.check_call(f"""
                docker run --rm --name {self.container_name} -p {self.port}:3030 \
                    -e ADMIN_PASSWORD={self.admin_pwd} \
                    -e JVM_ARGS=-Xmx4g \
                    -u $(id -u):$(id -g) \
                    -v {FUSEKI_DIR}:/fuseki -v {self.data_dir}:/data \
                    -d stain/jena-fuseki""", shell=True)

        return self

    def stop(self) -> 'Fuseki':
        """Stop Fuseki service"""
        subprocess.call(f"docker stop {self.container_name}",
                        stderr=subprocess.STDOUT,
                        shell=True)
        print("Stopping Fuseki service.", end="", flush=True)
        while self.is_started():
            time.sleep(0.1)
            print(".", end="", flush=True)
        print(" Finish!")
        return self

    def load_files(self, db_name: str, files_pattern: List[str]):
        conf_file = FUSEKI_DIR + f"/configuration/{db_name}.ttl"
        if self.data_dir is None:
            raise Exception("Need to start Fuseki service first")

        if not os.path.exists(conf_file):
            content = f"""
@prefix :      <http://base/#> .
@prefix tdb:   <http://jena.hpl.hp.com/2008/tdb#> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ja:    <http://jena.hpl.hp.com/2005/11/Assembler#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix fuseki: <http://jena.apache.org/fuseki#> .

:service_tdb_all  a                   fuseki:Service ;
        rdfs:label                    "TDB {db_name}" ;
        fuseki:dataset                :tdb_dataset_readwrite ;
        fuseki:name                   "{db_name}" ;
        fuseki:serviceQuery           "query" , "sparql" ;
        fuseki:serviceReadGraphStore  "get" ;
        fuseki:serviceReadWriteGraphStore
                "data" ;
        fuseki:serviceUpdate          "update" ;
        fuseki:serviceUpload          "upload" .

:tdb_dataset_readwrite
        a             tdb:DatasetTDB ;
        tdb:location  "/fuseki/databases/{db_name}" .
"""
            with open(conf_file, "w") as f:
                f.write(content)

        need_restart = self.is_started()
        if need_restart:
            self.stop()

        files = " ".join([f"/data/{s}" for s in files_pattern])
        exec_and_print(f"""
docker run --rm \
    -u $(id -u):$(id -g) \
    -v {FUSEKI_DIR}:/fuseki \
    -v {self.data_dir}:/data \
    stain/jena-fuseki ./load.sh {db_name} {files}""")

        if need_restart:
            self.start(self.data_dir)

    def is_started(self) -> bool:
        """Check if the fuseki container is up and running"""
        resp = subprocess.check_output(f"docker ps --filter 'name={self.container_name}' --format='{{{{.ID}}}}'",
                                       stderr=subprocess.STDOUT, shell=True)
        return resp.decode().strip() != ""

    def set_dbname(self, dbname: str) -> 'Fuseki':
        self.db_name = dbname
        return self

    @property
    def sparql(self):
        assert self.db_name is not None
        return SPARQLWrapper(f"http://localhost:{self.port}/{self.db_name}/sparql")

    def describe(self, query: str):
        sparql = self.sparql
        sparql.setQuery(query)
        return sparql.query().convert()

    def query(self, query: str):
        sparql = self.sparql
        sparql.setQuery(query)
        sparql.setReturnFormat("json")
        return sparql.query().convert()


def exec_and_print(cmd: str):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        print(stdout_line, end="")
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)
    return return_code
