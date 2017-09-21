'''
'''

import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from googleapiclient.http import MediaFileUpload 

import mimetypes
import logging

def get_credentials(config):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(config["client_secret"],
                                              config["scopes"])
        flow.user_agent = config["application_name"]
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        logging.debug('Storing credentials to %s',credential_path)
    return credentials

def save_to_drive(local_path,config):
    '''
    '''
    
    parent = config["base-directory-id"]
    target_repo = config["target-repo"]
    
    # Fire up the Drive API connection
    credentials = get_credentials(config)
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http, cache_discovery=False)
    files_hook = service.files()
    
    # Get the repo ID
    repos = files_hook.list(q="'"+parent+"' in parents").execute()
    repo_id = None
    for repo in repos.get('files',[]):                
        if repo["name"] == target_repo:
            repo_id = repo["id"]
    logging.debug("Got repo id %s",repo_id)
    
    # Get the files within the repo
    repo_files = {}
    page_token = None
    while True:
        # Check whether we are beyond the first page
        param = {}
        if page_token:
            param['pageToken'] = page_token
        # List all files in this repo
        children = files_hook.list(q="'"+repo_id+"' in parents",**param).execute()
        for child in children.get('files',[]):
            name = child["name"]
            if name in repo_files:
                raise IOError("Multiple IDs with name "+name)
            repo_files[name] = child["id"]
        # Check if there is another page
        page_token = children.get('nextPageToken')
        if not page_token:
            break

    # Upload a file
    name = os.path.split(local_path)[-1]
    mimetype,_ = mimetypes.guess_type(local_path)
    body = dict(name=name,mimeType=mimetype)
    media = MediaFileUpload(local_path,mimetype=mimetype,
                            resumable=True)
    # If the file already exists, then update
    if name in repo_files:
        f = files_hook.update(fileId=repo_files[name],body=body,
                              media_body=media).execute()
    # Otherwise, create
    else:
        body["parents"] = [repo_id]
        f = files_hook.create(body=body,media_body=media).execute()
    logging.debug("Updated file %s",f)

