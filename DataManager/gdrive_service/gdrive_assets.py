import inspect
import os

import gdown
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from DataManager import tempDir
from zipfile import ZipFile
from os.path import basename


class GDriveManage:
    def __init__(self):
        self.gauth = GoogleAuth()
        self.drive = GoogleDrive(self.gauth)
        # self.authenticate()

    def authenticate(self):
        self.gauth.LoadCredentialsFile("mycreds.txt")
        if self.gauth.credentials is None:
            # Authenticate if they're not there
            self.gauth.LocalWebserverAuth()
        elif self.gauth.access_token_expired:
            # Refresh them if expired
            self.gauth.Refresh()
        else:
            # Initialize the saved creds
            self.gauth.Authorize()
        # Save the current credentials to a file
        self.gauth.SaveCredentialsFile("mycreds.txt")

    def get_folder_id(self, folder_name):
        folder_list = self.drive.ListFile({'q': "trashed=false"}).GetList()
        for folder in folder_list:
            if folder['title'] == folder_name:
                file_list = self.drive.ListFile({'q': f"'{folder['id']}' in parents and trashed=false"}).GetList()
                for file in file_list:
                    file.Trash()
                return folder['id']
        return ''

    def edit_file(self, file_id):
        fileList = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
        for files in fileList:
            print('title: %s, id: %s' % (files['title'], files['id']))
            if files['title'] == 'user_info.txt':
                files.GetContentFile("user_info.txt")
                update = files.GetContentString() + "\ntest"
                files.SetContentString(update)
                files.Upload()
                break

    def upload_file_to_specific_folder(self, abs_path_file, folder_name='DataManager_Data'):
        folder_id = self.get_folder_id(folder_name)
        file_metadata = {'title': basename(abs_path_file), "parents": [{"id": folder_id, "kind": "drive#childList"}]}
        file_create = self.drive.CreateFile(file_metadata)
        file_create.SetContentFile(abs_path_file)  # The contents of the file
        file_create.Upload()

    def download_folder(self, folderID='12oipyI87bJYLMayYDl5afXzb9PFdpaQD', file_name='temp_files.zip'):
        url = f'https://drive.google.com/uc?id={folderID}'
        output = 'temp_files.zip'
        gdown.download(url, output, quiet=False)

    def make_zip_file(self, list_files, name_zip_file):
        zipObj = ZipFile(name_zip_file, 'w')

        for file in list_files:
            zipObj.write(file, basename(file))

        zipObj.close()


tempDirPath = os.path.dirname(inspect.getfile(tempDir))
assetDbFilePath = os.path.join(tempDirPath, 'AssetDB.db')
stockDataDbFilePath = os.path.join(tempDirPath, 'Stock_DataDB.db')

p = GDriveManage()
p.make_zip_file([assetDbFilePath, stockDataDbFilePath], os.path.join(tempDirPath, 'all.zip'))
p.upload_file_to_specific_folder(os.path.join(tempDirPath, 'all.zip'), 'DataManager_Data')
p.download_folder('13KWfNBfj3X2Yz8RNKRVBi1IcOT6BR7q1')
