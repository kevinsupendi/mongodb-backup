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
        self.shard_threads = []
        self.repl_threads = []
        self.cfg = None

        with open("config_replica.yaml", 'r') as ymlfile:
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
        for replset in targets:
            for replica in replset:
                host_client = paramiko.SSHClient()
                host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

                # check mongorestore
                stdin, stdout, stderr = host_client.exec_command('type mongorestore')
                found = False
                for _ in stdout:
                    found = True

                if not found:
                    host_client.close()
                    raise Exception("mongorestore command not found on target host")

                # check restic
                # stdin, stdout, stderr = host_client.exec_command('type restic')
                # found = False
                # for _ in stdout:
                #     found = True
                #
                # if not found:
                #     host_client.close()
                #     raise Exception("restic command not found on target host")

                print(replica["mongo_host"] + " OK")
                host_client.close()
        print("Requirements OK")

    def full_restore(self, replica, timestamp):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

        # search backup dir for full backup and log backup
        # get full backup nearest but earlier timestamp
        # check if full backup timestamp is in range of any log timestamp, if not raise Exception

        # Begin restore

        # Shutdown all mongod

        # Copy files from full backup dir to one node

        # Start standalone mongod, delete local database, then shutdown mongod

        # Copy files from full backup dir to other remaining nodes

        # Start and initiate replicaset

        # Add other node as members

        # Find primary node

        # Copy log backup to primary

        # Replay oplog on primary

        # Restore Done!

    def dedup_restore(self, replica, timestamp):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

        # restic restore

    def run_restore_replset(self, replset, mode, timestamp):
        for replica in replset:
            if mode == 'full':
                print("Running full restore")
                self.repl_threads.append(threading.Thread(target=self.full_restore, args=(replica, timestamp)))
            elif mode == 'dedup':
                print("Running dedup restore")
                self.repl_threads.append(threading.Thread(target=self.dedup_restore, args=(replica, timestamp)))
            else:
                raise Exception("Invalid restore mode")

        # Start all threads
        for thread in self.repl_threads:
            thread.start()

        # Wait for all of them to finish
        for thread in self.repl_threads:
            thread.join()

    def run_restore(self, mode, timestamp):
        if self.cfg['mongo_type'] == 'shard':
            # disable balancers
            pass

        for replset in self.targets:
            self.shard_threads.append(threading.Thread(target=self.run_restore_replset,
                                                       args=(replset, mode, timestamp)))

        # Start all threads
        for thread in self.shard_threads:
            thread.start()

        # Wait for all of them to finish
        for thread in self.shard_threads:
            thread.join()

        if self.cfg['mongo_type'] == 'shard':
            # enable balancers
            pass
