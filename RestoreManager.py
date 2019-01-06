from pymongo import MongoClient
from pymongo import WriteConcern
from pymongo.errors import OperationFailure
from distutils.version import StrictVersion
import yaml
import paramiko
import threading
import time


class RestoreManager:

    def __init__(self):
        self.version = None
        self.shard_threads = []
        self.repl_barrier = None
        self.cfg = None

        with open("config.yaml", 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile)

        self.version = str(self.cfg["mongo_version"])
        if self.cfg['mongo_type'] == 'replica':
            self.check_replset_requirements(self.cfg)
        elif self.cfg['mongo_type'] == 'shard':
            self.check_replset_requirements(self.cfg['config_servers'])
            for shard in self.cfg['shards']:
                self.check_replset_requirements(shard)
        else:
            raise Exception("Invalid mongo_type in config file")

    def enable_balancers(self):
        mongos_client = MongoClient(host=self.cfg['mongos_host'], port=self.cfg['mongos_port'],
                                    username=self.cfg['mongos_user'], password=self.cfg['mongos_pass'],
                                    authSource=self.cfg['mongos_auth_db'],
                                    serverSelectionTimeoutMS=self.cfg['server_timeout'])
        db = mongos_client.config
        db.settings.update({'_id': 'balancer'}, {'$set': {'stopped': False}}, upsert=True)
        mongos_client.close()

    def repair_permission(self, replica):
        # repair permission
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

        stdin, stdout, stderr = host_client.exec_command('systemctl stop mongod')
        stdout.channel.recv_exit_status()
        time.sleep(3)

        stdin, stdout, stderr = host_client.exec_command('chown -R mongodb:mongodb ' + replica['mongo_db_path'])
        stdout.channel.recv_exit_status()
        time.sleep(2)
        stdin, stdout, stderr = host_client.exec_command('chown -R mongodb:mongodb ' + replica['mongo_db_path'])
        stdout.channel.recv_exit_status()
        time.sleep(2)
        stdin, stdout, stderr = host_client.exec_command('chown -R mongodb:mongodb ' + replica['mongo_db_path'])
        stdout.channel.recv_exit_status()
        time.sleep(2)
        stdin, stdout, stderr = host_client.exec_command('systemctl start mongod')
        stdout.channel.recv_exit_status()
        time.sleep(3)

    def check_replset_requirements(self, config):
        # Trying to ssh to all member in ReplicaSet
        for replica in config['replicas']:
            host_client = paramiko.SSHClient()
            host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

            stdin, stdout, stderr = host_client.exec_command('type mongorestore')
            found = False
            for _ in stdout:
                found = True

            if not found:
                host_client.close()
                raise Exception("mongorestore command not found on target host")

            stdin, stdout, stderr = host_client.exec_command('type aws')
            found = False
            for _ in stdout:
                found = True

            if not found:
                host_client.close()
                raise Exception("aws command not found on target host")

    def full_restore(self, replica, timestamp, barrier, replset, ignore_log):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

        # search backup dir for full backup and log backup
        # get full backup nearest preceding timestamp
        temp_client = paramiko.SSHClient()
        temp_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        temp_client.connect(replset['target_host'], username=replica['ssh_user'], password=replica['ssh_pass'])

        stdin, stdout, stderr = temp_client.exec_command('aws s3 ls s3://'+self.cfg['s3_bucket_name']+'/' + replset['replica_name'] + '/full/')
        nearest_prec = 0
        for line in stdout:
            try:
                if int(line.split('/')[-2]) < timestamp:
                    nearest_prec = int(line.split('/')[-2])
            except ValueError:
                print("Ignoring unknown folder")

        if nearest_prec == 0:
            raise Exception('Full backup not found')

        log_range = ''
        if not ignore_log:
            # check if full backup timestamp is in range of any log timestamp, if not raise Exception
            stdin, stdout, stderr = temp_client.exec_command('aws s3 ls s3://'+self.cfg['s3_bucket_name']+'/' + replset['replica_name'] + '/log/')
            for line in stdout:
                try:
                    range_start = int(line.split('/')[-2].split("-")[0])
                    range_end = int(line.split('/')[-2].split("-")[1])
                    if range_start < nearest_prec < range_end:
                        log_range = line.split('/')[-2]
                        log_range = log_range.replace('\n', '')
                        break
                except ValueError:
                    print("Ignoring unknown folder")

            if log_range == '':
                raise Exception('No valid log to restore this timestamp')

            # check if restore timestamp within the log range
            range_start = int(log_range.split("-")[0])
            range_end = int(log_range.split("-")[1])
            if range_start < timestamp < range_end:
                pass
            else:
                raise Exception('Most recent backup not enough to restore this timestamp')

        print("Begin Restore ", replica["mongo_host"])
        stdin, stdout, stderr = host_client.exec_command('cat /tmp/checkpoint_restore')
        stdout.channel.recv_exit_status()
        checkpoint_level = 0
        try:
            checkpoint_level = int(stdout.read().strip())
        except ValueError:
            stdin, stdout, stderr = host_client.exec_command('touch /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()
            stdin, stdout, stderr = host_client.exec_command('echo "0" > /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()

        # CHECKPOINT 1
        if checkpoint_level < 1:
            # Shutdown all mongod
            stdin, stdout, stderr = host_client.exec_command('systemctl stop mongod')
            stdout.channel.recv_exit_status()

            # Clean mongo data directory
            stdin, stdout, stderr = host_client.exec_command('rm -r ' + replica['mongo_db_path'] + '/*')
            stdout.channel.recv_exit_status()

            # Copy files from full backup dir
            start = time.clock()
            stdin, stdout, stderr = host_client.exec_command('aws s3 cp --page-size 10000 --recursive s3://'+self.cfg['s3_bucket_name']+'/' +
                                                             replset['replica_name']+ '/full/' + str(nearest_prec) + '/ ' +
                                                             replica['mongo_db_path'] + '/')
            stdout.channel.recv_exit_status()
            end = time.clock()
            print("Download time for ", replica["mongo_host"], " ", str(end - start))

            stdin, stdout, stderr = host_client.exec_command('rm ' + replica['mongo_db_path'] + '/mongod.lock')
            stdout.channel.recv_exit_status()
            barrier.wait()

            # set checkpoint 1
            stdin, stdout, stderr = host_client.exec_command('echo "1" > /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()

        # CHECKPOINT 2
        if checkpoint_level < 2:
            # Stop all mongod
            stdin, stdout, stderr = host_client.exec_command('systemctl stop mongod')
            stdout.channel.recv_exit_status()
            # Start standalone mongod, delete local database, then shutdown mongod
            host_client.exec_command('mongod --dbpath '+replica['mongo_db_path'] + ' --port ' +str(replica['mongo_port']), get_pty=True)
            # wait until mongod ready to serve
            while True:
                try:
                    test_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                              username=replica['mongo_user'], password=replica['mongo_pass'],
                                              authSource=replica['mongo_auth_db'],
                                              serverSelectionTimeoutMS=self.cfg['server_timeout'])
                    test_client.list_database_names()
                    break
                except:
                    # Connect error, retrying
                    pass
            test_client.drop_database('local')
            test_client.close()
            host_client.close()

            # repair permission
            self.repair_permission(replica)

            # Start all mongod
            host_client = paramiko.SSHClient()
            host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])
            stdin, stdout, stderr = host_client.exec_command('systemctl start mongod')
            stdout.channel.recv_exit_status()
            barrier.wait()

            # set checkpoint 2
            stdin, stdout, stderr = host_client.exec_command('echo "2" > /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()

        # CHECKPOINT 3
        # PRIMARY ONLY
        # Find primary node
        if checkpoint_level < 3:
            self.repair_permission(replica)
            if replica['mongo_host'] == replset["target_host"]:
                # Initiate single node replicaset
                mongo_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                           username=replica['mongo_user'], password=replica['mongo_pass'],
                                           authSource=replica['mongo_auth_db'],
                                           serverSelectionTimeoutMS=self.cfg['server_timeout'])

                # if config servers, use additional parameter
                if self.cfg["mongo_type"] == "shard" and replset['replica_name'] == self.cfg['config_servers']['replica_name']:
                        config = {'_id': replset['replica_name'],
                                  'configsvr': True,
                                  'members': [{'_id': 0, 'host': replica['mongo_host'] + ':' + str(replica['mongo_port'])}]
                                  }
                else:
                    config = {'_id': replset['replica_name'],
                              'members': [{'_id': 0, 'host': replica['mongo_host']+':'+str(replica['mongo_port'])}]
                              }

                try:
                    mongo_client.admin.command("replSetInitiate", config)
                except OperationFailure:
                    print("Replicaset already initialized")

                # Add other node as members
                db = mongo_client['admin']

                # wait for node to be master
                while not mongo_client.is_primary:
                    time.sleep(1)

                for repl in replset["replicas"]:
                    if repl['mongo_host'] != replica['mongo_host']:
                        db.eval('rs.add("' + str(repl['mongo_host']) + ':' + str(repl['mongo_port'])+'")')

            barrier.wait()

            # set checkpoint 3
            stdin, stdout, stderr = host_client.exec_command('echo "3" > /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()

        if checkpoint_level < 4:
            self.repair_permission(replica)
            # CHECKPOINT 4
            # Clear per-shard sharding recovery information, ignore config_server
            if self.cfg['mongo_type'] == 'shard' and replset['replica_name'] != self.cfg['config_servers']['replica_name']:
                stdin, stdout, stderr = host_client.exec_command('systemctl stop mongod')
                stdout.channel.recv_exit_status()
                time.sleep(3)
                # restart all mongod with recoverShardingState parameter False
                if StrictVersion(self.version) > StrictVersion('3.4'):
                    host_client.exec_command('mongod --dbpath ' + replica['mongo_db_path'] + ' '
                                             '--setParameter=recoverShardingState=false --shardsvr --port ' +
                                             str(replica['mongo_port']) + ' --replSet ' + replset['replica_name'],
                                             get_pty=True)
                else:
                    host_client.exec_command('mongod --dbpath ' + replica['mongo_db_path'] + ' --port ' + str(replica['mongo_port']) +
                                             ' --setParameter=recoverShardingState=false --replSet ' +
                                             replset['replica_name'],
                                             get_pty=True)

                mongo_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                           username=replica['mongo_user'], password=replica['mongo_pass'],
                                           authSource=replica['mongo_auth_db'],
                                           replicaset=replset['replica_name'],
                                           serverSelectionTimeoutMS=self.cfg['server_timeout'])

                # Finding primary
                while mongo_client.primary is None:
                    time.sleep(1)

                primary = mongo_client.primary

                if replica['mongo_host'] == primary[0] and replica['mongo_port'] == primary[1]:
                    mongo_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                               username=replica['mongo_user'], password=replica['mongo_pass'],
                                               authSource=replica['mongo_auth_db'],
                                               serverSelectionTimeoutMS=self.cfg['server_timeout'])
                    collection = mongo_client.admin.system.version
                    coll2 = collection.with_options(write_concern=WriteConcern(w='majority'))
                    coll2.delete_one({"_id": "minOpTimeRecovery"})
                    print("Config cleared")
                barrier.wait()
                host_client.close()

                # restart shard normally
                time.sleep(3)
                self.repair_permission(replica)

            # set checkpoint 4
            host_client = paramiko.SSHClient()
            host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            host_client.connect(replica['mongo_host'], username=replica['ssh_user'], password=replica['ssh_pass'])
            stdin, stdout, stderr = host_client.exec_command('echo "4" > /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()

        # CHECKPOINT 5
        if checkpoint_level < 5:
            if not ignore_log:
                self.repair_permission(replica)
                mongo_client = MongoClient(host=replica['mongo_host'], port=replica['mongo_port'],
                                           username=replica['mongo_user'], password=replica['mongo_pass'],
                                           authSource=replica['mongo_auth_db'],
                                           replicaset=replset['replica_name'],
                                           serverSelectionTimeoutMS=self.cfg['server_timeout'])

                while mongo_client.primary is None:
                    time.sleep(1)

                primary = mongo_client.primary

                # PRIMARY ONLY
                if replica['mongo_host'] == primary[0] and replica['mongo_port'] == primary[1]:
                    # Create new tmp directory
                    stdin, stdout, stderr = host_client.exec_command('mkdir -p /tmp/oplog/')
                    stdout.channel.recv_exit_status()

                    # Copy log backup to primary
                    start = time.clock()
                    stdin, stdout, stderr = host_client.exec_command(
                        'aws s3 cp --page-size 10000 s3://'+self.cfg['s3_bucket_name']+'/' +
                        replset['replica_name'] + '/log/' + str(log_range) + '/oplog.bson /tmp/oplog/oplog.bson')
                    stdout.channel.recv_exit_status()
                    end = time.clock()
                    print("Log download time for ", replica["mongo_host"], " ", str(end - start))

                    # Replay oplog on primary
                    start = time.clock()
                    stdin, stdout, stderr = host_client.exec_command('mongorestore -u ' + replica['mongo_user'] + ' -p ' +
                                                                     replica['mongo_pass'] + ' --authenticationDatabase ' +
                                                                     replica['mongo_auth_db'] + ' --oplogReplay --oplogLimit ' +
                                                                     str(timestamp)+':1 --dir /tmp/oplog/')
                    stdout.channel.recv_exit_status()
                    end = time.clock()
                    print("Log replay time for ", replica["mongo_host"], " ", str(end - start))

                    # delete tmp directory
                    stdin, stdout, stderr = host_client.exec_command('rm -r /tmp/oplog/')
                    stdout.channel.recv_exit_status()

            barrier.wait()
            # set checkpoint 5
            stdin, stdout, stderr = host_client.exec_command('echo "5" > /tmp/checkpoint_restore')
            stdout.channel.recv_exit_status()
        print("Restore done ", replica["mongo_host"])

    def remove_all_checkpoints(self):
        if self.cfg['mongo_type'] == 'replica':
            for replica in self.cfg["replicas"]:
                host_client = paramiko.SSHClient()
                host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                host_client.connect(replica['mongo_host'], username=replica['ssh_user'],
                                    password=replica['ssh_pass'])
                stdin, stdout, stderr = host_client.exec_command('rm /tmp/checkpoint_restore')
                stdout.channel.recv_exit_status()
        elif self.cfg['mongo_type'] == 'shard':
            for replica in self.cfg["config_servers"]["replicas"]:
                host_client = paramiko.SSHClient()
                host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                host_client.connect(replica['mongo_host'], username=replica['ssh_user'],
                                    password=replica['ssh_pass'])
                stdin, stdout, stderr = host_client.exec_command('rm /tmp/checkpoint_restore')
                stdout.channel.recv_exit_status()

            for replset in self.cfg['shards']:
                for replica in replset['replicas']:
                    host_client = paramiko.SSHClient()
                    host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    host_client.connect(replica['mongo_host'], username=replica['ssh_user'],
                                        password=replica['ssh_pass'])
                    stdin, stdout, stderr = host_client.exec_command('rm /tmp/checkpoint_restore')
                    stdout.channel.recv_exit_status()

    def run_restore_replset(self, replset, timestamp, barrier, ignore_log):
        repl_threads = []
        for replica in replset['replicas']:
            repl_threads.append(threading.Thread(target=self.full_restore, args=(replica, timestamp, barrier,
                                                                                      replset, ignore_log), daemon=True))

        # Start all threads
        for thread in repl_threads:
            thread.start()

        # Wait for all of them to finish
        for thread in repl_threads:
            thread.join()

    def run_restore(self, timestamp, ignore_log):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        host_client.connect(self.cfg['mongos_host'], username=self.cfg['ssh_user'],
                            password=self.cfg['ssh_pass'])
        # stop mongos
        stdin, stdout, stderr = host_client.exec_command('systemctl stop mongos')
        stdout.channel.recv_exit_status()

        if self.cfg['mongo_type'] == 'replica':
            self.shard_threads.append(threading.Thread(target=self.run_restore_replset,
                                                       args=(self.cfg, timestamp, threading.Barrier(len(self.cfg['replicas'])), ignore_log),
                                                       daemon=True))
        elif self.cfg['mongo_type'] == 'shard':
            self.shard_threads.append(threading.Thread(target=self.run_restore_replset,
                                                       args=(self.cfg['config_servers'], timestamp, threading.Barrier(len(self.cfg['config_servers']['replicas'])), ignore_log), daemon=True))
            for replset in self.cfg['shards']:
                self.shard_threads.append(threading.Thread(target=self.run_restore_replset,
                                                           args=(replset, timestamp, threading.Barrier(len(replset['replicas'])), ignore_log), daemon=True))
        else:
            raise Exception("Invalid mongo_type in config file")

        # Start all threads
        for thread in self.shard_threads:
            thread.start()

        # Wait for all of them to finish
        for thread in self.shard_threads:
            thread.join()

        # start mongos
        stdin, stdout, stderr = host_client.exec_command('systemctl start mongos')
        stdout.channel.recv_exit_status()

        print("Cluster shard restore done")

        # Remove all checkpoints
        self.remove_all_checkpoints()
        self.enable_balancers()
