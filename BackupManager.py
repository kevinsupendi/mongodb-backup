from pymongo import MongoClient
from multiprocessing.pool import ThreadPool
import datetime
import yaml
import paramiko
import threading
import time
import traceback
import os
import subprocess
import sys

class BackupManager:

    def __init__(self):
        self.snapshot_limit = 0.0
        self.targets = []
        self.threads = []
        self.mongos = dict()
        self.mongos_client = None
        self.cfg = None

        with open("config.yaml", 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile)

        if self.cfg['mongo_type'] == 'replica':
            # set target node
            self.targets.append(self.validate_replset_config(self.cfg))
            # print(self.targets)
            self.check_requirements()
        elif self.cfg['mongo_type'] == 'shard':
            self.validate_mongos_config()
            self.targets.append(self.validate_replset_config(self.cfg['config_servers']))
            for shard in self.cfg['shards']:
                self.targets.append(self.validate_replset_config(shard))
            # print(self.targets)
            self.check_requirements()
        else:
            raise Exception("Invalid mongo_type in config file")
    
    def get_min_sec(self,duration):		
        min = duration // 60		
        sec = duration % 60		
        return min,sec		
			
    def get_hr_min_sec(self,duration):  
        if duration >= 60 and duration < 3600:
            hr = 0
            min,sec = self.get_min_sec(duration)		
        elif duration >= 3600:		
            hr = duration // 3600		
            min,sec = self.get_min_sec(duration % 3600)		
        else:		
            hr = 0		
            min = 0		
            sec = duration		
        return hr,min,sec

    def validate_mongos_config(self):
        # Validating mongos
        mongos_client = MongoClient(host=self.cfg['mongos_host'], port=self.cfg['mongos_port'],
                                    username=self.cfg['mongos_user'], password=self.cfg['mongos_pass'],
                                    authSource=self.cfg['mongos_auth_db'],
                                    serverSelectionTimeoutMS=self.cfg['server_timeout'])
        if mongos_client.is_mongos:
            self.mongos['mongos_host'] = self.cfg['mongos_host']
            self.mongos['mongos_port'] = self.cfg['mongos_port']
            self.mongos['mongos_user'] = self.cfg['mongos_user']
            self.mongos['mongos_pass'] = self.cfg['mongos_pass']
            self.mongos['mongos_auth_db'] = self.cfg['mongos_auth_db']
        else:
            raise Exception("Client is not mongos")
        self.mongos_client = mongos_client

    def validate_replset_config(self, config):
        # Connect to target host
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
                    target['mongo_db_path'] = replica['mongo_db_path']
                    target['mongo_user'] = replica['mongo_user']
                    target['mongo_pass'] = replica['mongo_pass']
                    target['mongo_auth_db'] = replica['mongo_auth_db']
                    target['ssh_user'] = replica['ssh_user']
                    target['ssh_pass'] = replica['ssh_pass']
                    target['lvm_volume'] = config['lvm_volume']
                    target['replica_name'] = config['replica_name']
                    break
                except Exception as e:
                    print(e)
                    test_client.close()
                    raise Exception("Failed to connect to target host")

        test_client.close()
        return target

    def check_requirements(self):
        for target in self.targets:
            # check lvm volume exist
            output = os.system('sudo lvscan | grep ' + target["lvm_volume"])
            if output != 0:
                raise Exception("LVM Volume not found on target host")
            
            # check filesystem remaining space for snapshot
            # free space must have at least 10% size of mongodb volume
            output =  subprocess.Popen("sudo pvs --units g | tail -n1 | awk '{print $6}'", shell=True, stdout=subprocess.PIPE).stdout
            output = output.read()
            output = output.decode()
            output = output.splitlines()
            free = float(output[0][:-1])

            volume = target["lvm_volume"].split("/")[-1]
            output =  subprocess.Popen("sudo lvs --units g | grep " + volume + " | awk '{print $4}'", shell=True, stdout=subprocess.PIPE).stdout
            output = output.read()
            output = output.decode()
            output = output.splitlines()
            mongo_size = float(output[0][:-1])

            self.snapshot_limit = mongo_size * self.cfg['percent_snap_limit']

            if free < self.snapshot_limit:
                raise Exception("Remaining disk space is less than 10% of mongodb volume")

            print(target["mongo_host"] + " OK")

    # def convert_to_bytes(self, target, input):
    #     if 'T' in input:

    def send_failed_notif(self):
        os.system("/home/syseng/mongodb-backup/SendNotif send --message \"Full Backup Mongodb " + self.cfg['replica_name'] 
                     + " is fail in. Please check the log.\"")

    def cek_backup(self):
        output = subprocess.Popen("sudo find "+self.cfg['cephfs_dir']+" * -mtime -1 -type d -exec du -s {} \;|grep "+self.cfg['cephfs_dir']+". |awk '{print $1}'", shell=True, stdout=subprocess.PIPE).stdout
        output = output.read()
        output = output.decode()
        sizeToday = output.splitlines()
        try:
            if int(sizeToday[0]) < 2500000:
                self.send_failed_notif()
            else:
                print("ok1")
        except:
            self.send_failed_notif()
            
        #print("clean up ",sizeToday[0])

    def full_backup(self, target, mode):
        try:
            start = time.time()
            ts = int(time.time())
            print("Backing up ", target["mongo_host"])
            vg = target['lvm_volume'].split("/")[2]
            # remove backup older than target['max_retention'] days
            output = subprocess.Popen("sudo find "+self.cfg['cephfs_dir']+" * -mtime +"+str(self.cfg['max_retention'])+" -type d -printf '%P\n'", shell=True, stdout=subprocess.PIPE).stdout
            output = output.read()
            output = output.decode()
            data = output.splitlines()
            print("clean up ",data)
            os.system("sudo find "+self.cfg['cephfs_dir']+" * -mtime +"+str(self.cfg['max_retention'])+" -type d -exec rm -rf {} \; 2> /dev/null")
     
            # create LVM snapshot of target volume
            os.system("sudo lvcreate --size " + str(self.snapshot_limit) +
                  "g --snapshot --name mdb-snap01 "
                  + target['lvm_volume'])

            # mount LVM snapshot
            os.system("sudo mkdir -p /tmp/lvm/snapshot")
            os.system("sudo mkdir -p "+self.cfg['cephfs_dir']+str(ts))
            os.system("sudo mount -o nouuid /dev/" + vg + "/mdb-snap01 /tmp/lvm/snapshot")
        
        
            output = subprocess.Popen("find /tmp/lvm/snapshot -type f -printf '%P\n'", shell=True, stdout=subprocess.PIPE).stdout
            output = output.read()
            output = output.decode()
            data = output.splitlines()
        except:
            self.send_failed_notif()

        # multiprocess upload
        print("Backing...")
        pool = ThreadPool(5)
        try:
            def upload_file(filebytes):
                filename = filebytes                
                os.system("cp /tmp/lvm/snapshot/"+filename+" "+self.cfg['cephfs_dir']+str(ts))
            pool.map(upload_file, data, chunksize=1)
        except:
            self.send_failed_notif()
        finally:  # To make sure processes are closed in the end, even if errors happen
            print("closed")
            pool.close()
            pool.join()

        end = time.time()
        elps = int(end-start)
        hr,min,sec = self.get_hr_min_sec(elps)
        print("Upload time for ", target['mongo_host'], " ", str(end - start))
        
        self.cek_backup()

        if mode == 'test':
            os.system("/home/syseng/mongodb-backup/SendNotif send --message \"test Full Backup Mongodb " + self.cfg['replica_name'] 
                     + " is success in " + str(hr)
                     + " hours " + str(min) + " minutes and " + str(sec) + " seconds.\"")
        
        os.system("sudo umount /tmp/lvm/snapshot")
        os.system("sudo lvremove -f "+vg+"/mdb-snap01")

        print("Backup done! ", target["mongo_host"])

    def log_backup(self, target, period):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])
        print("Backing up ", target["mongo_host"])

        # mongodump to backup dir
        ts_end = int(time.time())
        ts_start = ts_end - period

        stdin, stdout, stderr = host_client.exec_command('export HTTPS_PROXY=http://172.18.26.146:80;'
                                                         'export HTTP_PROXY=http://172.18.26.146:80;'
                                                         'aws s3 mb s3://' + self.cfg['s3_bucket_name'] + '/')
        stdout.channel.recv_exit_status()

        filename = str(ts_start) + '-' + str(ts_end)

        start = time.clock()
        stdin, stdout, stderr = host_client.exec_command('mongodump -u '+target['mongo_user']+' -p '+target['mongo_pass']+' --authenticationDatabase '+target['mongo_auth_db']+' --db=local --collection=oplog.rs --query \''
                                                         '{ "ts" :{ "$gte" : Timestamp('+str(ts_start)+',1) }, "ts" : '
                                                         '{ "$lte" : Timestamp('+str(ts_end)+',1) } }'
                                                         '\' --out - > /data/temp/oplog.bson')
        stdout.channel.recv_exit_status()
        end = time.clock()
        print("Upload dump time for ", target["mongo_host"], " ", str(end - start))

        print("Backup done! ", target["mongo_host"])

    def log_backup_daily(self, target):
        host_client = paramiko.SSHClient()
        host_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #host_client.connect(target['mongo_host'], username=target['ssh_user'], password=target['ssh_pass'])
        print("Backing up ", target["mongo_host"])

        # mongodump to backup dir
        ts_end = int(time.time())
        midnight = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
        ts_start = int(time.mktime(midnight.timetuple()))

        filename = str(ts_start) + '-' + str(ts_end)
        start = time.clock()
        stdin, stdout, stderr = host_client.exec_command('export HTTPS_PROXY=http://172.18.26.146:80;'
                                                         'export HTTP_PROXY=http://172.18.26.146:80;'
                                                         'mongodump --user '+target['mongo_user']+' --password '+target['mongo_pass']+' --authenticationDatabase '+target['mongo_auth_db']+' --db=local --collection=oplog.rs --query \''
                                                         '{ "ts" :{ "$gte" : Timestamp(' + str(
            ts_start) + ',1) }, "ts" : '
                        '{ "$lte" : Timestamp(' + str(ts_end) + ',1) } }'
                                                                '\' --out - | aws s3 cp - s3://' + self.cfg[
                                                             's3_bucket_name'] + '/' + target[
                                                             'replica_name'] + '/log/' +
                                                         filename + '/oplog.bson')
        stdout.channel.recv_exit_status()
        end = time.clock()
        print("Upload dump time for ", target["mongo_host"], " ", str(end - start))

        # remove previous daily backup
        stdin, stdout, stderr = host_client.exec_command('export HTTPS_PROXY=http://172.18.26.146:80;'
                                                         'export HTTP_PROXY=http://172.18.26.146:80;'
                                                         'aws s3 ls s3://'+self.cfg['s3_bucket_name']+'/' + target['replica_name'] + '/log/')
        log_range = ''
        for line in stdout:
            try:
                range_start = int(line.split('/')[-2].split("-")[0])
                if range_start == ts_start:
                    log_range = line.split('/')[-2]
                    log_range = log_range.replace('\n', '')
                    break
            except ValueError:
                print("Ignoring unknown folder")

        stdin, stdout, stderr = host_client.exec_command('export HTTPS_PROXY=http://172.18.26.146:80;'
                                                         'export HTTP_PROXY=http://172.18.26.146:80;'
                                                         'aws s3 rm --recursive s3://'+self.cfg['s3_bucket_name']+'/' + target['replica_name'] + '/log/' +
                                                         log_range + '/')
        stdout.channel.recv_exit_status()
        print("Backup done! ", target["mongo_host"])

    def enable_balancers(self):
        db = self.mongos_client.config
        db.settings.update({'_id': 'balancer'}, {'$set': {'stopped': False}}, upsert=True)

    def disable_balancers(self):
        db = self.mongos_client.config
        db.settings.update({'_id': 'balancer'}, {'$set': {'stopped': True}}, upsert=True)

    def run_backup(self, mode, log_period=7200):
        if self.cfg['mongo_type'] == 'shard':
            self.disable_balancers()

        for target in self.targets:
            if mode == 'full':
                self.threads.append(threading.Thread(target=self.full_backup, args=(target,'full')))
            elif mode == 'test':
                self.threads.append(threading.Thread(target=self.full_backup, args=(target,'test')))
            elif mode == 'log':
                self.threads.append(threading.Thread(target=self.log_backup, args=(target, log_period)))
            elif mode == 'log_daily':
                self.threads.append(threading.Thread(target=self.log_backup_daily, args=(target,)))
            elif mode == 'cek_backup':
                self.threads.append(threading.Thread(target=self.cek_backup, args=(target,)))
            else:
                raise Exception("Invalid backup mode")

        # Start all threads
        for thread in self.threads:
            thread.start()

        # Wait for all of them to finish
        for thread in self.threads:
            thread.join()

        if self.cfg['mongo_type'] == 'shard':
            self.enable_balancers()
