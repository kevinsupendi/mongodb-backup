from BackupManager import BackupManager

if __name__ == '__main__':
    print('Starting MongoDB Backup Script')

    manager = BackupManager()
    print(manager.client.list_database_names())
