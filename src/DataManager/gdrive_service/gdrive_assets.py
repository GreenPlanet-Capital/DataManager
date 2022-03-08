import inspect
import os

import gdown
from pydrive.drive import GoogleDrive
from DataManager import config_files, tempDir
from zipfile import ZipFile
from os.path import basename
from pydrive.auth import GoogleAuth


class GDriveManage:
    def __init__(self):
        self.gauth = GoogleAuth()
        self.drive = GoogleDrive(self.gauth)
        self.temp_dir = os.path.dirname(inspect.getfile(tempDir))
        self.config_files_dir = os.path.dirname(inspect.getfile(config_files))
        GoogleAuth.DEFAULT_SETTINGS["client_config_file"] = os.path.join(
            self.config_files_dir, "client_secrets.json"
        )

    def authenticate(self):
        if os.path.exists(os.path.join(self.temp_dir, "mycreds.txt")):
            self.gauth.LoadCredentialsFile(os.path.join(self.temp_dir, "mycreds.txt"))
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
        self.gauth.SaveCredentialsFile(os.path.join(self.temp_dir, "mycreds.txt"))

    def get_list_files(self, folder_):
        return self.drive.ListFile(
            {"q": f"'{folder_['id']}' in parents and trashed=false"}
        ).GetList()

    def get_list_folders(self):
        return self.drive.ListFile({"q": "trashed=false"}).GetList()

    def clear_contents(self, folder_obj, to_exclude_files):
        for file in self.get_list_files(folder_obj):
            if file["title"] not in to_exclude_files:
                file.Trash()

    def get_folder_object(self, folder_name):
        folder_list = self.get_list_folders()
        for folder in folder_list:
            if folder["title"] == folder_name:
                return folder

    def edit_file(self, folder_obj, file_name, to_insert):
        list_files = self.get_list_files(folder_obj)
        to_edit_file = [f for f in list_files if f["title"] == file_name][0]
        to_edit_file.SetContentString(f"{to_insert}")
        to_edit_file.Upload()
        return to_edit_file["id"]

    def upload_file_to_specific_folder(self, abs_path_file, folder_obj):
        folder_id = folder_obj["id"]
        file_metadata = {
            "title": basename(abs_path_file),
            "parents": [{"id": folder_id, "kind": "drive#childList"}],
        }
        file_create = self.drive.CreateFile(file_metadata)
        file_create.SetContentFile(abs_path_file)
        file_create.Upload()
        return file_create["id"]

    @staticmethod
    def download_folder(folderID, folder_name):
        url = f"https://drive.google.com/uc?id={folderID}"
        gdown.download(url, folder_name, quiet=False)

    @staticmethod
    def make_zip_file(list_files, name_zip_file):
        zipObj = ZipFile(name_zip_file, "w")
        for file in list_files:
            zipObj.write(file, basename(file))
        zipObj.close()

    @staticmethod
    def unzip_files(path_to_zip_file, directory_to_extract_to):
        with ZipFile(path_to_zip_file, "r") as zip_ref:
            zip_ref.extractall(directory_to_extract_to)


# Global vars
tempDirPath = os.path.dirname(inspect.getfile(tempDir))
assetDbFilePath = os.path.join(tempDirPath, "AssetDB.db")
stockDataDbFilePath = os.path.join(tempDirPath, "Stock_DataDB.db")
data_file_id = "1Cob1f-iq_d5Ytfc6PhdDJFoZrk3gNVzO"
datamgr_folder = None


def upload_files():
    g_manager = GDriveManage()
    g_manager.authenticate()

    # Zip db files & clear remote dir contents
    global datamgr_folder
    if datamgr_folder is None:
        datamgr_folder = g_manager.get_folder_object("DataManager_Data")
    g_manager.make_zip_file(
        [assetDbFilePath, stockDataDbFilePath], os.path.join(tempDirPath, "all.zip")
    )
    g_manager.clear_contents(datamgr_folder, {"data_info.txt"})

    # Get id of uploaded zip file
    id_all_zip = g_manager.upload_file_to_specific_folder(
        os.path.join(tempDirPath, "all.zip"), datamgr_folder
    )
    g_manager.edit_file(datamgr_folder, "data_info.txt", id_all_zip)


def init_remote():
    g_manager = GDriveManage()
    g_manager.authenticate()
    # Zip db files & clear remote dir contents
    global datamgr_folder
    if datamgr_folder is None:
        datamgr_folder = g_manager.get_folder_object("DataManager_Data")

    # Get id of uploaded zip file
    open(os.path.join(tempDirPath, "data_info.txt"), "a").close()
    _ = g_manager.upload_file_to_specific_folder(
        os.path.join(tempDirPath, "data_info.txt"), datamgr_folder
    )


def download_files():
    # Hard-code id below to data_info.txt
    GDriveManage.download_folder(
        data_file_id, os.path.join(tempDirPath, "data_info.txt")
    )
    data_ = open(os.path.join(tempDirPath, "data_info.txt"), "r")

    # Download zip file with db files
    GDriveManage.download_folder(data_.readline(), os.path.join(tempDirPath, "all.zip"))
    GDriveManage.unzip_files(os.path.join(tempDirPath, "all.zip"), tempDirPath)
