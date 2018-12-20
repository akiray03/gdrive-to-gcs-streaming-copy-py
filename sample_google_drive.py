import os
import sys
import tempfile

import google_auth_httplib2
from httplib2 import Http

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage

from streaming_copy import google_drive
from streaming_copy import google_cloud_storage


SERVICE_ACCOUNT_PATH = os.environ.get('SERVICE_ACCOUNT_FILEPATH')
SCOPES = ['https://www.googleapis.com/auth/drive']


def get_drive_service():
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH)
    scoped_credentials = credentials.with_scopes(scopes=SCOPES)
    http = google_auth_httplib2.AuthorizedHttp(scoped_credentials, http=Http())
    drive_service = build('drive', 'v3', http=http)
    return drive_service


def get_downloader_from_google_drive(drive_service, file_id):
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

    return metadata, downloader


def get_uploader_to_google_cloud_storage(bucket_name, object_path, content_type, total_size):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    destination_blob = bucket.blob(object_path)

    resumable_url = destination_blob.create_resumable_upload_session(
        content_type=content_type,
        size=total_size,
        client=storage_client,
    )

    uploader = google_cloud_storage.MediaPartialUploader(
        http=Http(),
        resumable_url=resumable_url,
        content_type=content_type,
        total_size=total_size,
    )

    return uploader


def copy(drive_file_id, bucket_name, object_path):
    drive_service = get_drive_service()
    metadata, downloader = get_downloader_from_google_drive(
        drive_service=drive_service,
        file_id=drive_file_id
    )

    uploader = get_uploader_to_google_cloud_storage(
        bucket_name=bucket_name,
        object_path=object_path,
        content_type=metadata['mimeType'],
        total_size=int(metadata['size']),
    )

    counter = 1
    done = False
    while done is False:
        print('file content copying counter={counter}'.format(counter=counter))
        with tempfile.NamedTemporaryFile() as fd:
            progress = downloader.fetch_next_chunk(fd=fd)
            done = progress.done

            fd.seek(0)
            payload = fd.read()

            uploader.upload_next_chunk(
                payload=payload,
                chunk_size=len(payload)
            )

    if not uploader.done:
        raise RuntimeError('Uploader is not finished.')

    print('copy finished.')


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('[usage] {progname} <Google Drive FileID> <GCS Bucket Name> <GCS Object Path>'.format(
            progname=os.path.basename(__file__)
        ))
        exit(1)

    drive_file_id = sys.argv[1]
    bucket_name = sys.argv[2]
    object_path = sys.argv[3]

    copy(drive_file_id=drive_file_id, bucket_name=bucket_name, object_path=object_path)

