import os
import re
import subprocess
from pathlib import Path

import click

from downloader import download_sat_comp_if_not_exist

CURRENT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))


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


def generate_kissat_scripts(task_output_dir, script_output_dir, time_limit_s, kissat_path, logs):
    for file_name in os.listdir(task_output_dir):
        file_path = task_output_dir / file_name
        log_file_path = logs / f"kissat_{file_name}.log"
        script = script_output_dir / f"kissat_{file_name}_with_limit_{time_limit_s}.sh"
        with open(script, "w") as file:
            file.write("#!/bin/bash -i \n")
            file.write("source activate multithreading_solver \n")
            file.write(f"{kissat_path} --time={time_limit_s} {file_path} >> {log_file_path}")


def generate_multithreading_solver_scripts(task_output_dir, script_output_dir, time_limit_s,
                                           multithreading_solver_path, logs,
                                           multithreading_solver_tmp, multithreading_solver_log,
                                           multithreading_solver_redis_dump):
    base_port = 6381
    for i, file_name in enumerate(os.listdir(task_output_dir)):
        file_path = task_output_dir / file_name
        log_file_path = logs / f"multithreading_solver_{file_name}.log"
        multithreading_solver_tmp_dir = multithreading_solver_tmp / file_name
        multithreading_solver_log_dir = multithreading_solver_log / file_name
        redis_dir = multithreading_solver_redis_dump / file_name
        create_directory(redis_dir)
        redis_port = base_port + i
        script = script_output_dir / f"multithreading_solver_{file_name}_with_limit_{time_limit_s}_with_redis_port_{redis_port}.sh"
        with open(script, "w") as file:
            file.write("#!/bin/bash -i \n")
            file.write("source activate multithreading_solver \n")
            file.write("export LD_LIBRARY_PATH=~/hiredis-1.2.0/install/lib:$LD_LIBRARY_PATH\n")
            file.write(f"cd {multithreading_solver_path.parent}\n")
            file.write(f"redis-server redis.conf --port {redis_port} --dir {redis_dir} &\n")
            file.write("PID=$!\n")
            file.write(
                f"python {multithreading_solver_path} {file_path} --max-learning 15 --redis-port {redis_port} --max-buffer-size 10000 -tmp {multithreading_solver_tmp_dir} --log-dir {multithreading_solver_log_dir} -n 5 -er 1 -er 1 -er 1 -er 1 -er 1 -es 8 -es 10 -es 12 -es 14 -es 14 -ei 800 -ei 1000 -ei 1200 -ei 2000 -ei 3000 -c 0 -c 0 -c 0 -c 0 -c 0 >> {log_file_path} \n")
            file.write("kill -9 $PID\n")


def run_scripts(scripts_output_dir, time_limit_s, script_prefix, cpus=1, mem=10):
    priority = 5000
    hours = time_limit_s // 60 // 60 + 1
    jobs = []
    for file_name in os.listdir(scripts_output_dir):
        if script_prefix in file_name:
            scripts_path = scripts_output_dir / file_name
            command = ["sbatch", f"--cpus-per-task={cpus}", f"--mem={mem}G", f"--time={hours}:00:00",
                       f"--priority={priority}", scripts_path]

            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)

            pattern = re.compile(r'Submitted batch job (\d+)')
            match = pattern.search(result.stdout)

            if match:
                job_id = match.group(1)
            else:
                raise Exception("Job ID not found in the output.")
            jobs.append((job_id, file_name, cpus, mem, hours))
            priority = max(100, priority - 10)
    return jobs


def print_log(jobs_log, log_prefix):
    log = CURRENT_DIR / f"{log_prefix}.log"
    with open(log, "w") as log_file:
        log_file.write(f"job_id:file_name:cpus:mem:hours\n")

        for job_id, file_name, cpus, mem, hours in jobs_log:
            log_file.write(f"{job_id}:{file_name}:{cpus}:{mem}:{hours}\n")


def cancel_job(job_id):
    command = ["scancel", job_id]
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    if result.returncode == 0:
        print(f"Задача {job_id} была отменена")
    else:
        print(f"Задача {job_id} не была отменена. Какая-то проблемма")


def cancel_jobs(log_file):
    with open(log_file, "r") as kissat_log_file:
        for line in kissat_log_file:
            if "job_id" in line:
                continue
            job_id, *_ = line.split(":")
            cancel_job(job_id)


@click.command()
@click.argument('uri_task_path', required=True, type=click.Path(exists=True))
@click.option('--kissat-path', required=True, type=click.Path(exists=True))
@click.option('--multithreading-solver-path', required=True, type=click.Path(exists=True))
@click.option('--time-limit-s', type=int, default=5000, help='time limit for solver')
@click.option('--task-output-dir', type=str, default="sta_comp_2023",
              help='sat comp 2023 task output dir')
@click.option('--scripts-output-dir', type=str, default="scripts_output_dir",
              help='output dir for run scripts')
@click.option('--logs', type=click.Path(exists=False), default="logs",
              help='logs dir')
@click.option('--multithreading-solver-rubbish-dir',
              type=click.Path(exists=False), default="/mnt/tank/scratch/aandreev/")
def run_experiments(uri_task_path: str, kissat_path: str, multithreading_solver_path: str,
                    time_limit_s: int, task_output_dir: str, scripts_output_dir: str, logs: str,
                    multithreading_solver_rubbish_dir: str):
    uri_task_path = Path(uri_task_path)
    kissat_path = Path(kissat_path)
    multithreading_solver_path = Path(multithreading_solver_path)
    task_output_dir = CURRENT_DIR / task_output_dir
    scripts_output_dir = CURRENT_DIR / scripts_output_dir
    logs = CURRENT_DIR / logs
    multithreading_solver_rubbish_dir = Path(multithreading_solver_rubbish_dir)
    multithreading_solver_tmp = multithreading_solver_rubbish_dir / "tmp"
    multithreading_solver_log = multithreading_solver_rubbish_dir / "log"
    multithreading_solver_redis_dump = multithreading_solver_rubbish_dir / "redis_dump"

    download_sat_comp_if_not_exist(uri_task_path, task_output_dir)

    clean_directory(scripts_output_dir)
    create_directory(scripts_output_dir)
    clean_directory(logs)
    create_directory(logs)
    clean_directory(multithreading_solver_tmp)
    clean_directory(multithreading_solver_log)
    clean_directory(multithreading_solver_redis_dump)

    kissat_experiments_log = CURRENT_DIR / "kissat.log"
    if os.path.exists(kissat_experiments_log):
        cancel_jobs(kissat_experiments_log)

    multithreading_solver_experiments_log = CURRENT_DIR / "multithreading_solver.log"
    if os.path.exists(multithreading_solver_experiments_log):
        cancel_jobs(multithreading_solver_experiments_log)

    # generate_kissat_scripts(task_output_dir, scripts_output_dir, time_limit_s, kissat_path, logs)
    # jobs_log = run_scripts(scripts_output_dir, time_limit_s, "kissat", cpus=1, mem=10)
    # print_log(jobs_log, "kissat")

    generate_multithreading_solver_scripts(task_output_dir, scripts_output_dir,
                                           time_limit_s, multithreading_solver_path, logs,
                                           multithreading_solver_tmp, multithreading_solver_log,
                                           multithreading_solver_redis_dump)

    jobs_log = run_scripts(scripts_output_dir, time_limit_s, "multithreading_solver", cpus=6, mem=20)
    print_log(jobs_log, "multithreading_solver")


if __name__ == "__main__":
    run_experiments()
