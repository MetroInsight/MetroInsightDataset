from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys
import os

# Data Sources
data_dir_str_list = ['buildingdepot', 'google_traffic', 'ion_data']
#data_dir_str_list = ['google_traffic']

# Init Google Drive
gauth = GoogleAuth()
auth_url = gauth.GetAuthUrl()
print "Visit here: %s"%auth_url
code = raw_input()
gauth.Auth(code)
gdrive = GoogleDrive(gauth)

q = "title contains 'MetroInsightDataset' and mimeType = 'application/vnd.google-apps.folder'"
file_list = gdrive.ListFile({"q":q}).GetList()
root_dir = file_list[0]
parent_dir_id = root_dir['id']

def make_dir(gdrive, parent_dir_id, title):
    q = "title = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false"%(title, parent_dir_id)
    file_list = gdrive.ListFile({"q":q}).GetList()
    if len(file_list)>0:
        return file_list[0]
    else:
         dir_f = gdrive.CreateFile({"title":title, "parents":[{"id":parent_dir_id}], 'mimeType':'application/vnd.google-apps.folder'})
         dir_f.Upload()
         return dir_f

def upsert_file(gdrive, dir_id, title, input_filename):
    q = "title = '%s' and '%s' in parents and trashed = false"%(title, dir_id)
    file_list = gdrive.ListFile({"q":q}).GetList()
    for f in file_list:
        f.Delete()
    f = gdrive.CreateFile({"parents":[{"kind":"drive#fileLink", "id":data_dir['id']}], "title":title})
    #filename = data_dir_str + "/data/" + filename
    f.SetContentFile(input_filename)
    f.Upload()

    


for data_dir_str in data_dir_str_list:
    proj_dir = make_dir(gdrive, parent_dir_id, data_dir_str)
    data_dir = make_dir(gdrive, proj_dir['id'], 'data')
    for filename in os.listdir(data_dir_str + "/data"):
        if '.csv' in filename:
            print filename
            #f = gdrive.CreateFile({"parents":[{"kind":"drive#fileLink", "id":data_dir['id']}], "title":filename})
            #filename = data_dir_str + "/data/" + filename
            #f.SetContentFile(filename)
            #print f.Upload()
            title = filename
            input_filename = data_dir_str + "/data/" + filename
            upsert_file(gdrive, data_dir['id'], title, input_filename)
            
