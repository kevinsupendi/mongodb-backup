from BackupManager import BackupManager

if __name__ == '__main__':
    print('Starting MongoDB Backup Script')

    manager = BackupManager()
    print(manager.client.list_database_names())
    stdin, stdout, stderr = manager.host_client.exec_command('ls')
    for line in stdout:
        print(line.strip('\n'))
    manager.host_client.close()