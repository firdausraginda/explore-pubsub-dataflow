import os

def setup_creds():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './service_account.json'

    return None