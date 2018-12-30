import copy
import logging
import json
import os
import time
from ipaddress import ip_address
from kazoo.client import KazooClient

CLUSTER_NAME = "demo"

DEFAULT_DATA_PORT = 4440
DEFAULT_GOSSIP_PORT = 4441

ZOOKEEPER_HOST = "{}:{}".format(
    os.environ.get('ZOOKEEPER_SERVICE_HOST'),
    os.environ.get('ZOOKEEPER_SERVICE_PORT_CLIENT'))

CONFIG_PATH = "/{}.conf".format(CLUSTER_NAME)
LOCK_PATH = "{}.lock".format(CONFIG_PATH)

class LogdeviceStarter:

    def __init__(self):
        self.zk = KazooClient(hosts=ZOOKEEPER_HOST)
        self.zk.start()

    def get_config(self):
        logging.info("Trying to get: {}".format(CONFIG_PATH))
        if not self.zk.exists(CONFIG_PATH):
            logging.info("Config not found")
            return None
        data, stat = self.zk.get(CONFIG_PATH)
        data = data.decode("utf-8")
        logging.info("Got config: {}".format(data))
        return json.loads(data)
    
    def init_config(self):
        return copy.deepcopy(DEFAULT_CONFIG)
    
    def my_id(self):
        name = os.environ.get('POD_NAME')
        prefix = os.environ.get('POD_NAME_PREFIX')

        id = int(name[len(prefix):])
        return id
    
    def find_me(self, config, id):
        nodes = config["nodes"]
        me = [x for x in nodes if x["node_id"] == id]
        if not me:
            return None
        return me[0]

    def _parse_hostname(self, hostname):
        ip, separator, port = hostname.rpartition(':')
        assert separator
        port = int(port)
        ip = ip_address(ip.strip("[]"))
        return ip, port
    
    def update_me(self, config, id):
        me = self.find_me(config, id)
        if me is None:
            me = {
                "node_id": id,
                "host": "",
                "gossip_port": 0,
                "generation": 0,
                "roles": [ "sequencer", "storage" ],
                "sequencer": True,
                "storage": "read-write",
                "num_shards": 1
            }
            config["nodes"].append(me)
        dirty = False
        new_ip, new_port = ip_address(os.environ.get('POD_IP')), DEFAULT_DATA_PORT
        old_ip, old_port = None, None
        if me["host"] != "":
            old_ip, old_port = self._parse_hostname(me["host"])
        if (new_ip != old_ip) or (new_port != old_port):
            dirty = True
            if new_ip.version == 6:
                host = "[{}]:{}".format(new_ip, new_port)
            else:
                host = "{}:{}".format(new_ip, new_port)
            me["host"] = host
        
        if me["gossip_port"] != DEFAULT_GOSSIP_PORT:
            dirty = True
            me["gossip_port"] = DEFAULT_GOSSIP_PORT
        
        if dirty:
            me["generation"] += 1
    
    def set_config(self, config):
        serialized = json.dumps(config)
        logging.info("Setting new config: {}".format(serialized))
        serialized = serialized.encode('utf-8')
        if not self.zk.exists(CONFIG_PATH):
            self.zk.create(CONFIG_PATH, serialized)
        else:
            self.zk.set(CONFIG_PATH, serialized)
    
    def prepare_logdevice_directories(self):
        # Assumes that pod spec mounts the volume at /data/logdevice/shard0
        with open("/data/logdevice/NSHARDS", "w") as f:
            f.write("1")

    def start_logdevice(self):
        # Replace the running binary
        os.execv(
            "/usr/local/bin/logdeviced",
            [
                "/usr/local/bin/logdeviced",
                "--config-path=zk:{}{}".format(ZOOKEEPER_HOST, CONFIG_PATH),
                "--port={}".format(DEFAULT_DATA_PORT),
                "--local-log-store-path=/data/logdevice/",
            ]
        )
    
    def run(self):
        logging.info("Trying to acquire lock file: {}".format(LOCK_PATH))
        id = self.my_id()
        config_lock = self.zk.Lock(LOCK_PATH, "N{}".format(id))
        
        with config_lock:
            logging.info("Acquired lock file: {}".format(LOCK_PATH))
            config = self.get_config()
            if config is None:
                config = self.init_config()
            
            self.update_me(config, id)
            self.set_config(config)
        
        self.prepare_logdevice_directories()
        self.start_logdevice()

DEFAULT_CONFIG = {
    "cluster": CLUSTER_NAME,
    "nodes": [],
    "internal_logs": {
        "config_log_deltas": {
            "replicate_across": {
                "node": 3
            }
        },
        "config_log_snapshots": {
            "replicate_across": {
                "node": 3
            }
        },
        "event_log_deltas": {
            "replicate_across": {
                "node": 3
            }
        },
        "event_log_snapshots": {
            "replicate_across": {
                "node": 3
            }
        }
    },
    "metadata_logs": {
        "nodeset": [0,1,2,3],
        "replicate_across": { "node": 3 }
    },
    "zookeeper": {
        "quorum": [
          ZOOKEEPER_HOST,
        ],
        "timeout": "30s"
    }
}

logging.basicConfig(level=logging.INFO)
logging.info(os.environ)
LogdeviceStarter().run()
