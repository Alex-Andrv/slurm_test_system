import os

import requests
import lzma
import io

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