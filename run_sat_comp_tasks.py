import os
import re
import subprocess
from pathlib import Path

import click

from downloader import download_sat_comp_if_not_exist

def clean_directory(directory):
    try:
        subprocess.run(["rm", "-r", directory])
        print(f"Все файлы и папки в {directory} удалены успешно.")
    except Exception as e:
        print(f"Произошла ошибка при удалении файлов и папок: {e}")

def create_directory(directory_path):
    try:
        # Пытаемся создать директорию
        os.makedirs(directory_path)
        print(f"Директория {directory_path} успешно создана.")
    except FileExistsError:
        print(f"Директория {directory_path} уже существует.")
    except Exception as e:
        print(f"Произошла ошибка при создании директории: {e}")

def generate_kissat_scripts(output_dir, time_limit_s, kissat_path, logs):
    for file_name in os.listdir(output_dir):
        file_path = output_dir / file_name
        log_file_path = logs / f"kissat_{file_name}.log"
        with open(f"kissat_{file_name}_with_limit_{time_limit_s}.sh") as file:
            file.write("#!/bin/bash -i \n")
            file.write("source activate multithreading_solver \n")
            file.write(f"{kissat_path} --time={time_limit_s} {file_path} >> {log_file_path}")


def run_kissat_scripts(scripts_output_dir, time_limit_s, script_prefix, cpus=1, mem=10):
    hours = time_limit_s / 60 / 60 + 1
    jobs = []
    for file_name in os.listdir(scripts_output_dir):
        if script_prefix in file_name:
            scripts_path = scripts_output_dir / file_name
            command = ["sbatch", f"--cpus-per-task={cpus}", f"--mem={mem}G", f"--time={hours}:00:00", scripts_path]
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)

            pattern = re.compile(r'Submitted batch job (\d+)')
            match = pattern.search(result.stdout)

            if match:
                job_id = match.group(1)
            else:
                raise Exception("Job ID not found in the output.")

            jobs.append((job_id, file_name, cpus, mem, hours))
    return jobs


def print_log(jobs_log, logs, log_prefix):
    log = logs / log_prefix
    with open(log, "w") as log_file:
        log_file.write(f"job_id:file_name:cpus:mem:hours")

        for job_id, file_name, cpus, mem, hours in jobs_log:
            log_file.write(f"{job_id}:{file_name}:{cpus}:{mem}:{hours}\n")


@click.command()
@click.argument('uri_task_path', required=True, type=click.Path(exists=True))
@click.option('--kissat-path', required=True, type=click.Path(exists=True))
@click.option('--multithreading-solver-path', required=True, type=click.Path(exists=True))
@click.option('--time-limit-s', type=int, default=5000, help='time limit for solver')
@click.option('--task-output-dir', type=click.Path(exists=False), default="sta_comp_2023",
              help='sat comp 2023 task output dir')
@click.option('--scripts-output-dir', type=click.Path(exists=False), default="scripts_output_dir",
              help='output dir for run scripts')
@click.option('--logs', type=click.Path(exists=False), default="logs",
              help='logs dir')
def run_experiments(uri_task_path: Path, kissat_path: Path, multithreading_solver_path: Path,
                    time_limit_s: int, task_output_dir: Path, scripts_output_dir: Path, logs: Path):
    download_sat_comp_if_not_exist(uri_task_path, task_output_dir)
    clean_directory(scripts_output_dir)
    create_directory(scripts_output_dir)
    generate_kissat_scripts(scripts_output_dir, time_limit_s, kissat_path, logs)
    jobs_log = run_kissat_scripts(scripts_output_dir, time_limit_s, "kissat")
    print_log(jobs_log, logs, "kissat")


if __name__ == "__main__":
    run_experiments()
