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
            self.targets = []
            self.targets.append(self.validate_replset_config(self.cfg))
            print(self.targets)
            self.check_requirements(self.targets)
        elif self.cfg['mongo_type'] == 'shard':
            self.targets = []
            self.targets.append(self.validate_replset_config(self.cfg['config_servers']))
            for shard in self.cfg['shards']:
                self.targets.append(self.validate_replset_config(shard))
            print(self.targets)
            self.check_requirements(self.targets)
        else:
            raise Exception("Invalid mongo_type in config file")

    def validate_replset_config(self, config):
        print("Trying to connect to one of ReplicaSet")
        target = dict()
        test_client = None
        for replica in config['replicas']:
            if replica['mongo_host'] == config['target_host']:
                try:
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
                    break
                except Exception as e:
                    print(e)
                    raise Exception("Failed to connect to target host")
        print("Successfully connected to target host")

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
        return target

    def check_requirements(self, targets):
        for target in targets:
            host_client = paramiko.SSHClient()
            host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])

            # check LVM
            # check filesystem remaining space for snapshot
            # check mongodump & mongorestore
            # check lvm volume exist

            # (?) borgbackup

    def backup(self, targets):
        # TODO: USE THREADING
        for target in targets:
            host_client = paramiko.SSHClient()
            host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])

            if self.cfg['backup_mode'] == 'normal':
                # backup with cp
                pass
            elif self.cfg['backup_mode'] == 'dedup':
                # backup with borgbackup
                pass
            else:
                raise Exception("Invalid backup_mode in config file")
