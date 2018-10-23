from pymongo import MongoClient
import yaml
import paramiko
import threading
import time


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
        self.snapshot_limit = 0.0
        self.targets = []
        self.threads = []
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
                    test_client.close()
                    raise Exception("Failed to connect to target host")
        print("Successfully connected to target host")

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
        return target

    def check_requirements(self, targets):
        print("Checking requirements")
        for target in targets:
            host_client = paramiko.SSHClient()
            host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])

            # check LVM
            stdin, stdout, stderr = host_client.exec_command('type lvs')
            found = False
            for _ in stdout:
                found = True

            if not found:
                host_client.close()
                raise Exception("LVM command not found on target host")

            # check lvm volume exist
            stdin, stdout, stderr = host_client.exec_command('lvscan | grep ' + self.cfg["lvm_volume"])
            found = False
            for _ in stdout:
                found = True

            if not found:
                host_client.close()
                raise Exception("LVM Volume not found on target host")

            # check filesystem remaining space for snapshot
            # free space must have at least 10% size of mongodb volume
            stdin, stdout, stderr = host_client.exec_command("pvs --units g | tail -n1 | awk '{print $6}'")
            free = 0.0
            for line in stdout:
                free = float(line[:-2])

            volume = self.cfg["lvm_volume"].split("/")[-1]
            stdin, stdout, stderr = host_client.exec_command("lvs --units g | grep " + volume + " | awk '{print $4}'")
            mongo_size = 0.0
            for line in stdout:
                mongo_size = float(line[:-2])

            self.snapshot_limit = mongo_size / 10
            if free < self.snapshot_limit:
                host_client.close()
                raise Exception("Remaining disk space is less than 10% of mongodb volume")

            # check mongodump
            stdin, stdout, stderr = host_client.exec_command('type mongodump')
            found = False
            for _ in stdout:
                found = True

            if not found:
                host_client.close()
                raise Exception("mongodump command not found on target host")

            # check restic
            # stdin, stdout, stderr = host_client.exec_command('type restic')
            # found = False
            # for _ in stdout:
            #     found = True
            #
            # if not found:
            #     host_client.close()
            #     raise Exception("restic command not found on target host")

            print(target["mongo_host"] + " OK")
            host_client.close()
        print("Requirements OK")

    def full_backup(self, target):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])

        print("Backing up ", target["mongo_host"])

        vg = self.cfg['lvm_volume'].split("/")[2]

        print("create LVM snapshot of target volume")
        stdin, stdout, stderr = host_client.exec_command('lvcreate --size ' + str(self.snapshot_limit) + 'g --snapshot --name mdb-snap01 '
                                 + self.cfg['lvm_volume'])
        stdout.channel.recv_exit_status()
        ts = int(time.time())

        print("mount LVM snapshot")
        stdin, stdout, stderr = host_client.exec_command('mkdir -p /tmp/lvm/snapshot')
        stdout.channel.recv_exit_status()
        stdin, stdout, stderr = host_client.exec_command('mount /dev/'+vg+'/mdb-snap01 /tmp/lvm/snapshot')
        stdout.channel.recv_exit_status()

        print("cp target mongodb_path to backup dir")
        stdin, stdout, stderr = host_client.exec_command('mkdir -p /backup/'+self.cfg['replica_name']+'/full/'+str(ts))
        stdout.channel.recv_exit_status()
        stdin, stdout, stderr = host_client.exec_command('cp -R /tmp/lvm/snapshot/* /backup/'+self.cfg['replica_name']+'/full/'+str(ts))
        stdout.channel.recv_exit_status()

        print("unmount LVM snapshot")
        stdin, stdout, stderr = host_client.exec_command('umount /tmp/lvm/snapshot')
        stdout.channel.recv_exit_status()

        print("delete LVM snapshot")
        stdin, stdout, stderr = host_client.exec_command('lvremove -f '+vg+'/mdb-snap01')
        stdout.channel.recv_exit_status()
        print("Backup done! ", target["mongo_host"])

    def log_backup(self, target, period):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])
        print("Backing up ", target["mongo_host"])

        # mongodump to backup dir
        # range log backup = 2 hours
        ts_end = int(time.time())
        ts_start = ts_end - period

        stdin, stdout, stderr = host_client.exec_command('mongodump --db=local --collection=oplog.rs --query \'{ "ts" : '
                                 '{ "$gte" : Timestamp('+str(ts_start)+',1) }, "ts" : '
                                 '{ "$lte" : Timestamp('+str(ts_end)+',1) } }'
                                 '\' --out - > oplog.bson')
        stdout.channel.recv_exit_status()

        filename = str(ts_start)+'-'+str(ts_end)+'.bson'
        stdin, stdout, stderr = host_client.exec_command('mkdir -p /backup/'+self.cfg['replica_name']+'/log/')
        stdout.channel.recv_exit_status()
        stdin, stdout, stderr = host_client.exec_command('mv oplog.bson /backup/'+self.cfg['replica_name']+'/log/'+filename)
        stdout.channel.recv_exit_status()
        print("Backup done! ", target["mongo_host"])

    def dedup_backup(self, target):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])
        print("Backing up ", target["mongo_host"])
        # restic backup
        print("Backup Done!")
        print("Backup done! ", target["mongo_host"])

    def run_backup(self, mode, log_period=7200):
        if self.cfg['mongo_type'] == 'shard':
            # disable balancers
            pass

        for target in self.targets:
            if mode == 'full':
                print("Running full backup")
                self.threads.append(threading.Thread(target=self.full_backup, args=(target,)))
            elif mode == 'log':
                print("Running log backup")
                self.threads.append(threading.Thread(target=self.log_backup, args=(target, log_period)))
            elif mode == 'dedup':
                print("Running dedup backup")
                self.threads.append(threading.Thread(target=self.dedup_backup, args=(target,)))
            else:
                raise Exception("Invalid backup mode")

            # Start all threads
            for thread in self.threads:
                thread.start()

            # Wait for all of them to finish
            for thread in self.threads:
                thread.join()

            if self.cfg['mongo_type'] == 'shard':
                # enable balancers
                pass
