from RestoreManager import RestoreManager
import argparse

parser = argparse.ArgumentParser(description='MongoDB Restore Script')
parser.add_argument('timestamp', type=int, help='timestamp to be restored')
parser.add_argument('--ignore-log', action='store_true',
                    help='option to ignore log_backup, use this if there is no valid log backup')
args = parser.parse_args()

manager = RestoreManager()
manager.run_restore(args.timestamp, args.ignore_log)
