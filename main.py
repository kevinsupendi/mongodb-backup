from BackupManager import BackupManager
from RestoreManager import RestoreManager

if __name__ == '__main__':
    print('Starting MongoDB Backup Script')

    manager = RestoreManager()
    manager.run_restore(1540277448)