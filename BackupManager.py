from Utils import Utils
from pymongo import MongoClient
import yaml
import paramiko


class BackupManager:

    """
    Class for managing BackupWorker and Restore Worker
    to do Backup/Restore.

    Before doing any Backup/Restore operation, BackupManager
    checks host ReplicaSet/Cluster status,
    check if the required programs (LVM, ZBackup, MongoDB) exist on host,
    then assign jobs to worker
    """
    def __init__(self):
        with open("config_replica.yaml", 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile)

        if self.cfg['mongo_type'] == 'replica':
            # set target node
            self.target = self.validate_replset_config(self.cfg)
            print(self.target)
        elif self.cfg['mongo_type'] == 'shard':
            self.target = []
            self.target.append(self.validate_replset_config(self.cfg['config_servers']))
            for shard in self.cfg['shards']:
                self.target.append(self.validate_replset_config(shard))
            print(self.target)
        else:
            raise Exception("Invalid mongo_type in config file")
        # self.host_client = paramiko.SSHClient()
        # self.host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # self.host_client.connect(self.cfg['mongo_host'], username=self.cfg['ssh_user'], password=self.cfg['ssh_pass'])

    def validate_replset_config(self, config):
        print("Trying to connect to one of ReplicaSet")
        test_client = None
        for replica in config['replicas']:
            try:
                test_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                               username=replica['mongo_user'], password=replica['mongo_pass'],
                                               authSource=replica['mongo_auth_db'],
                                               replicaset=config['replica_name'],
                                               serverSelectionTimeoutMS=self.cfg['server_timeout'])
                test_client.list_database_names()
                break
            except:
                print("Failed on host ", replica['mongo_host'], " trying again on another host")
        print("Successfully connected to one of ReplicaSet on address ", test_client.address)

        print("Validating config file")
        if len(test_client.nodes) != len(config['replicas']):
            raise Exception("Invalid number of replicas, ReplicaSet has " + str(len(test_client.nodes)) +
                            " replicas, but " + str(len(config['replicas'])) + " replicas in config file")

        for node in test_client.nodes:
            found = False
            for replica in config['replicas']:
                if node[0] == replica['mongo_host'] and node[1] == replica['mongo_port']:
                    found = True
                    break
            if not found:
                raise Exception("Invalid config, hostname or port from server didnt match with config file")
        print("Config file OK")

        # add ssh info
        address = test_client.secondaries.pop()
        target = {}
        target['mongo_host'] = address[0]
        target['mongo_port'] = address[1]
        for replica in config['replicas']:
            if target['mongo_host'] == replica['mongo_host'] and target['mongo_port'] == replica['mongo_port']:
                target['ssh_user'] = replica['ssh_user']
                target['ssh_pass'] = replica['ssh_pass']
                break
        return target

    def backup(self):
        pass
