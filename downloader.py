import os
from pathlib import Path

import click
import requests
import lzma
import io

CURRENT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))

def download_sat_comp_if_not_exist(track_main_2023_paths_path, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(track_main_2023_paths_path, 'r') as file:
        for line in file:
            url = line.strip()
            file_name = url.split("/")[-1]
            if os.path.exists(f"{output_folder}/{file_name}.cnf"):
                print(f"Файл '{file_name}' был скачан ранее")
                continue
            download_and_extract(url, output_folder)

def download_and_extract(url, output_folder):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            file_name = url.split("/")[-1]
            extracted_content = extract_content(response.content)
            file_name = os.path.splitext(file_name)[0]
            extracted_file_path = f"{output_folder}/{file_name}.cnf"
            with open(extracted_file_path, 'wb') as extracted_file:
                extracted_file.write(extracted_content)
            print(f"Файл '{file_name}' успешно скачан и разархивирован.")
        else:
            print(f"Не удалось скачать файл по URL: {url}, статус код: {response.status_code}")
    except Exception as e:
        print(f"Произошла ошибка при скачивании и разархивировании файла: {e}")

def extract_content(xz_content):
    try:
        with lzma.open(io.BytesIO(xz_content), 'rb') as xz_file:
            content = xz_file.read()
            return content
    except Exception as e:
        print(f"Произошла ошибка при разархивировании файла: {e}")
        return None

@click.command()
@click.argument('uri_task_path', required=True, type=click.Path(exists=True))
@click.option('--task-output-dir', type=str, default="sta_comp_2023",
              help='sat comp 2023 task output dir')
def download(uri_task_path: str, task_output_dir: str):
    uri_task_path = Path(uri_task_path)
    task_output_dir = CURRENT_DIR / task_output_dir
    download_sat_comp_if_not_exist(uri_task_path, task_output_dir)

if __name__ == "__main__":
    download()