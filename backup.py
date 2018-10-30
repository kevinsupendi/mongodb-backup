from BackupManager import BackupManager
import argparse

parser = argparse.ArgumentParser(description='MongoDB Backup Script')
parser.add_argument('backup_mode', help='backup mode can either be full, log or log_daily')
parser.add_argument('--period', action='store', type=int,
                    help='Used only for log backup mode. Specify an integer for how many seconds in the past to do log backup')
args = parser.parse_args()

manager = BackupManager()
if args.period is None:
    manager.run_backup(args.backup_mode)
else:
    manager.run_backup(args.backup_mode, args.period)
