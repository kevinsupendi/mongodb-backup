import BackupManager
import HostClient
from pymongo import MongoClient

if __name__ == '__main__':
    print('Starting MongoDB Backup Script')

    # MongoClient arguments
    host = '192.168.45.141'
    port = 27017
    username = 'admin'
    password = 'admin123'
    auth_database = 'admin'

    client = MongoClient(host=host, port=port, username=username, password=password, authSource=auth_database)
    print(client.list_database_names())
