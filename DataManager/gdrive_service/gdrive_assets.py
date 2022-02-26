import inspect
import os
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import shutil

from DataManager import tempDir


class GDriveManage:
    def __init__(self):
        self.gauth = GoogleAuth()
        self.drive = GoogleDrive(self.gauth)
        self.authenticate()

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
                return folder['id']
        return ''

    def upload_file_to_specific_folder(self, abs_path_file, folder_name='DataManager_Data'):
        folder_id = self.get_folder_id(folder_name)
        file_metadata = {'title': abs_path_file, "parents": [{"id": folder_id, "kind": "drive#childList"}]}
        file_create = self.drive.CreateFile(file_metadata)
        file_create.SetContentFile(abs_path_file)  # The contents of the file
        file_create.Upload()

    def downloadFolder(self, destinationFolder, fileId='12oipyI87bJYLMayYDl5afXzb9PFdpaQD'):
        file_list = self.drive.ListFile({'q': "'{}' in parents and trashed=false".format(fileId)}).GetList()
        for i, file1 in enumerate(sorted(file_list, key=lambda x: x['title']), start=1):
            print('Downloading {} from GDrive ({}/{})'.format(file1['title'], i, len(file_list)))
            file1.GetContentFile(file1['title'])
            shutil.move(file1['title'], os.path.join(destinationFolder, file1['title']))


# assetConfigFileName = os.path.dirname(inspect.getfile(tempDir))
#
# p = GDriveManage()
# p.downloadFolder(assetConfigFileName)
# p.upload_file_to_specific_folder('lmaoded.txt', 'Classroom')
