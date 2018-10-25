from BackupManager import BackupManager
from RestoreManager import RestoreManager

if __name__ == '__main__':
    print('Starting MongoDB Backup Script')

    manager = BackupManager()
    manager.run_backup('log')
