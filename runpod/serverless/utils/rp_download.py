'''
PodWorker | modules | download.py

Called when inputs are images or zip files.
Downloads them into a temporary directory called "input_objects".
This directory is cleaned up after the job is complete.
'''

import os
import re
import uuid
import zipfile
from typing import List, Union
from urllib.parse import urlparse
from email import message_from_string
from concurrent.futures import ThreadPoolExecutor

import httpx

def download_files_from_urls(job_id: str, urls: Union[str, List[str]]) -> List[str]:
    download_directory = os.path.abspath(os.path.join('jobs', job_id, 'downloaded_files'))
    os.makedirs(download_directory, exist_ok=True)

    def download_file(url: str) -> str:
        with httpx.Client() as client:
            response = client.get(url, timeout=5)
            response.raise_for_status()

            content_disposition = response.headers.get('Content-Disposition')
            msg = message_from_string(f'Content-Disposition: {content_disposition}')
            params = dict(msg.items()) if content_disposition else {}
            file_extension = os.path.splitext(params.get('filename', ''))[1]

            file_name = f'{uuid.uuid4()}{file_extension}'
            output_file_path = os.path.join(download_directory, file_name)

            with open(output_file_path, 'wb') as output_file:
                output_file.write(response.content)

        return os.path.abspath(output_file_path)

    if isinstance(urls, str):
        urls = [urls]

    with ThreadPoolExecutor() as executor:
        downloaded_files = list(executor.map(download_file, urls))

    return downloaded_files


def file(file_url: str) -> dict:
    '''
    Downloads a single file from a given URL, file is given a random name.
    First checks if the content-disposition header is set, if so, uses the file name from there.
    If the file is a zip file, it is extracted into a directory with the same name.
    Returns an object that contains:
    - The absolute path to the downloaded file
    - File type
    - Original file name
    '''
    os.makedirs('job_files', exist_ok=True)

    with httpx.Client() as client:
        download_response = client.get(file_url, timeout=30)

        original_file_name = []
        if "Content-Disposition" in download_response.headers.keys():
            original_file_name = re.findall(
                "filename=(.+)",
                download_response.headers["Content-Disposition"]
            )

        if len(original_file_name) > 0:
            original_file_name = original_file_name[0]
        else:
            download_path = urlparse(file_url).path
            original_file_name = os.path.basename(download_path)

        file_type = os.path.splitext(original_file_name)[1].replace('.', '')

        file_name = f'{uuid.uuid4()}'

        output_file_path = os.path.join('job_files', f'{file_name}.{file_type}')
        with open(output_file_path, 'wb') as output_file:
            output_file.write(download_response.content)

        if file_type == 'zip':
            unziped_directory = os.path.join('job_files', file_name)
            os.makedirs(unziped_directory, exist_ok=True)
            with zipfile.ZipFile(output_file_path, 'r') as zip_ref:
                zip_ref.extractall(unziped_directory)
            unziped_directory = os.path.abspath(unziped_directory)
        else:
            unziped_directory = None

    return {
        "file_path": os.path.abspath(output_file_path),
        "type": file_type,
        "original_name": original_file_name,
        "extracted_path": unziped_directory
    }

