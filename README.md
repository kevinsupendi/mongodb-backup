# mongodb-backup
This project was based on official MongoDB Backup [documentation](https://docs.mongodb.com/manual/core/backups/).
It uses the same backup & restore procedure written on the documentation.

mongodb-backup consists of two scripts, backup and restore.
Both scripts use configuration file in the same directory named config.yaml.

You can see config file example for [replica](config_replica.yaml) and [shard](config_shard.yaml)

Backups will be uploaded to S3. Make sure each VM used for backup has s3cmd configured.

Restore using the same config file will replace existing cluster with a new one. 
Restore script can also be used on empty nodes, and it will automatically initialise replication and sharding. 

## Table of Contents
1. [Features](#features)
2. [How Does it Work](#how-does-it-work)
2. [Requirements](#requirements)
3. [Usage](#usage)
4. [Configuration](#configuration)

## Features
- Backups can be performed while the database is running
- Restores can be used to replace existing cluster or deploy on empty nodes
- Point-In-Time Recovery using oplog
- Scripts use multithreading so each VM can start backup or restore procedure in parallel
- Restore script uses checkpointing so it can be resumed if restore
stuck or fail in the middle of the operation. Just run it again with the
same command.

## How does it work

### Backup
Backup operations only run on one target VM for each Replica Sets. For example if a shard has three Replica Sets
(including config servers), than it will use three threads to run
backup jobs on each Replica Sets. 

The target host for each Replica Sets must be specified in config.yaml
and it should be a hidden secondary member of the Replica Sets. LVM 
need not to be installed to all member in Replica Sets, only
target host need LVM for backup purposes.

Once it has known the target host, each thread will ssh to each target. 
Then it will perform backup operations :

**Full Backup**
1. Create LVM snapshot of mongodb LVM Volume
2. Mount snapshot volume
3. Upload mongodb data from snapshot volume to S3 (using s3cmd put -r)
4. Unmount and delete snapshot volume

Full backup will be saved in s3://backup/\<replicaset_name>/full/\<timestamp>/

Example : s3://backup/replica01/full/1540976296/

**Log Backup**
1. Mongodump from oplog collection in a given time range
2. Upload oplog.bson dump to S3 (using s3cmd put)

Log Backup will be saved in s3://backup/\<replicaset_name>/log/\<timestamp_range>/oplog.bson

Example : s3://backup/replica01/log/1540976296-1540983496/oplog.bson

### Restore
Restore operations run on all servers that are on config.yaml.
It can replace existing cluster or run on empty VM (as long as it has the requirements).

The script will run a thread for each server to do restore operations:

1. Clean mongo_db_path directory (specified in config file)
2. Download mongodb data from S3 to mongo_db_path
3. Delete **local** database to remove existing Replica Sets configuration
4. For each Replica Sets
   1. initialise new Replica Sets
   2. Clear per shard recovery information
   3. (Optional) Replay oplog on Primary member
   
If it is sharded cluster, the script will disable balancer before restore operation
and reenable it again after all operation finished successfully.

## Requirements
Requirements for each VM that is used for backup or restore

### Backup Requirements
- Create config.yaml
- MongoDB ( >= v3.2) 
- LVM
- MongoDB data directory mounted on LVM
- root user
- s3cmd configured for root user
- mongodump
- more than 10% free disk space

### Restore Requirements
- Create config.yaml
- MongoDB ( >= v3.2) 
- systemd and mongod.service
- mongod.conf for mongod.service systemd
- root user
- s3cmd configured for root user
- mongorestore

## Usage

### Backup

```
python backup.py -h

usage: backup.py [-h] [--period PERIOD] backup_mode

MongoDB Backup Script

positional arguments:
  backup_mode      backup mode can either be full, log or log_daily

optional arguments:
  -h, --help       show this help message and exit
  --period PERIOD  Used only for log backup mode. Specify an integer for how
                   many seconds in the past to do log backup
```

Example usage in crontab file

```
# daily full backup at 00.00
0 24 * * * /usr/bin/python backup.py full

# daily log backup at 00.00 updated every 2 hours
0 */2 * * * /usr/bin/python backup.py log_daily

# log backup every 2 hours
0 */2 * * * /usr/bin/python backup.py log --period 7200
```

### Restore

```
python restore.py -h

usage: restore.py [-h] [--ignore-log] timestamp

MongoDB Restore Script

positional arguments:
  timestamp     timestamp to be restored

optional arguments:
  -h, --help    show this help message and exit
  --ignore-log  option to ignore log_backup, use this if there is no valid log
                backup

```

```
# python restore.py 1540958167
# python restore.py 1540958167 --ignore-log
```

## Configuration

- mongo_type : "replica" or "shard"
- mongo_version : version number
- server_timeout : integer (milliseconds to wait for mongo server when connecting)
- s3_bucket_name : Bucket name used for backup and restore

### Shard
- shard_name : string
- mongos_host : IP address or hostname
- mongos_port : port number
- mongos_user : mongos username
- mongos_pass : mongos password
- mongos_auth_db : Database used for authentication
- ssh_user : root user
- ssh_pass : password for root user
- config_servers : [ReplicaSet](#replica-set)
- shards : List of [ReplicaSet](#replica-set) (Shard)

### Replica Set 
- replica_name : string
- target_host : IP address or hostname. Only used for backup script.
Specify which member to backup. Preferably a secondary hidden member.
- lvm_volume : string. Volume name that is used by mongodb, mounted in mongo_db_path
- replicas : List of [Replica](#replica) 

### Replica
- mongo_host : IP address or hostname
- mongo_port : port number
- mongo_user : username
- mongo_pass : password
- mongo_auth_db : Database used for authentication
- mongo_db_path : Path to use for backup. Must reside in LVM volume
- ssh_user : root user
- ssh_pass : password for root user
