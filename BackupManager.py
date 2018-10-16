from BackupWorker import BackupWorker
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
        with open("config.yaml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        self.client = MongoClient(host=cfg['mongo_host'], port=cfg['mongo_port'], username=cfg['mongo_user'],
                                  password=cfg['mongo_pass'], authSource=cfg['mongo_auth_db'])
        self.host_client = paramiko.SSHClient()
        self.host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.host_client.connect(cfg['mongo_host'], username=cfg['ssh_user'], password=cfg['ssh_pass'])

    def __del__(self):
        self.client.close()
        self.host_client.close()

    def check_backup_requirements(self):
        """
        Checklist
        - mongodump/restore tools
        - LVM
        - ZBackup
        :return bool:
        """
        pass

    def backup(self):
        # check if server is a mere ReplicaSet or Sharded Cluster
        if self.client.is_mongos:
            pass
        else:
            # ReplicaSet
            if self.client.is_primary:
                # find secondary member to run
                pass
            else:
                pass

    def restore(self):
        pass
