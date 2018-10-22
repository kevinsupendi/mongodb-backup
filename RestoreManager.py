from pymongo import MongoClient
import yaml
import paramiko
import threading
import time


class RestoreManager:

    """
    Class for managing BackupWorker and Restore Worker
    to do Backup/Restore.

    Before doing any Backup/Restore operation, BackupManager
    checks host ReplicaSet/Cluster status,
    check if the required programs (LVM, ZBackup, MongoDB) exist on host,
    then assign jobs to worker
    """
    def __init__(self):
        self.targets = []
        self.cfg = None

        with open("config_shard.yaml", 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile)

        if self.cfg['mongo_type'] == 'replica':
            # set target node
            self.targets.append(self.validate_replset_config(self.cfg))
            print(self.targets)
            self.check_requirements(self.targets)
        elif self.cfg['mongo_type'] == 'shard':
            self.targets.append(self.validate_replset_config(self.cfg['config_servers']))
            for shard in self.cfg['shards']:
                self.targets.append(self.validate_replset_config(shard))
            print(self.targets)
            self.check_requirements(self.targets)
        else:
            raise Exception("Invalid mongo_type in config file")

    def validate_replset_config(self, config):
        print("Trying to connect to all member in ReplicaSet")
        targets = []
        test_client = None
        for replica in config['replicas']:
            target = dict()
            test_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                      username=replica['mongo_user'], password=replica['mongo_pass'],
                                      authSource=replica['mongo_auth_db'],
                                      replicaset=config['replica_name'],
                                      serverSelectionTimeoutMS=self.cfg['server_timeout'])
            test_client.list_database_names()
            target['mongo_host'] = replica['mongo_host']
            target['mongo_port'] = replica['mongo_port']
            target['ssh_user'] = replica['ssh_user']
            target['ssh_pass'] = replica['ssh_pass']
            targets.append(target)
        print("Successfully connected to all host")

        print("Validating config file")
        if len(test_client.nodes) != len(config['replicas']):
            test_client.close()
            raise Exception("Invalid number of replicas, ReplicaSet has " + str(len(test_client.nodes)) +
                            " replicas, but " + str(len(config['replicas'])) + " replicas in config file")

        for node in test_client.nodes:
            found = False
            for replica in config['replicas']:
                if node[0] == replica['mongo_host'] and node[1] == replica['mongo_port']:
                    found = True
                    break
            if not found:
                test_client.close()
                raise Exception("Invalid config, hostname or port from server didnt match with config file")
        print("Config file OK")
        test_client.close()
        return targets

    def check_requirements(self, targets):
        print("Checking requirements")
        print("Requirements OK")

    def full_restore(self, target):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])

    def dedup_restore(self, target):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])

        # restic restore

    def run_restore(self, mode):
        for target in self.targets:
            if mode == 'full':
                print("Running full restore")
                threading.Thread(target=self.full_restore, args=(target,)).start()
            elif mode == 'dedup':
                print("Running dedup restore")
                threading.Thread(target=self.dedup_restore, args=(target,)).start()
            else:
                raise Exception("Invalid restore mode")
