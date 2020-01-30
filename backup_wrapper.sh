#!/bin/bash
cd /home/syseng/mongodb-backup;python3 backup.py full 1> /home/syseng/mongodb-backup/backup_$(date +"%Y%m%d").log 2> /home/syseng/mongodb-backup/backup_$(date +"%Y%m%d").err
