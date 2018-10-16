from BackupWorker import BackupWorker
from HostClient import HostClient
import HostClient
import yaml
from pymongo import MongoClient


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
        self.host_client = HostClient(host=cfg['mongo_host'], user=cfg['ssh_user'], password=cfg['ssh_pass'])

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
