import os
import sys
import google_auth_httplib2
from httplib2 import Http

from google.oauth2 import service_account
from googleapiclient.discovery import build

from streaming_copy import google_drive


SERVICE_ACCOUNT_PATH = os.environ.get('SERVICE_ACCOUNT_FILEPATH')
SCOPES = ['https://www.googleapis.com/auth/drive']


def streaming_copy_from_google_drive(file_id):
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
    scoped_credentials = credentials.with_scopes(scopes=SCOPES)
    http = google_auth_httplib2.AuthorizedHttp(scoped_credentials, http=Http())
    drive_service = build('drive', 'v3', http=http)

    metadata = drive_service.files().get(
        fileId=file_id,
        supportsTeamDrives=True,
        fields='*',
    ).execute()

    chunk_size = 10 * 1024 * 1024  # 10MB; GCPへのアップロードは256KBの倍数で指定する必要がある

    downloader = google_drive.MediaPartialDownloader(
        request=drive_service.files().get_media(
            fileId=file_id
        ),
        chunk_size=chunk_size
    )

    done = False
    with open(file_id, 'wb') as fd:
        progress = downloader.fetch_next_chunk(fd=fd)
        done = progress.done
        print(progress)

    print(metadata)


streaming_copy_from_google_drive(file_id=sys.argv[1])
