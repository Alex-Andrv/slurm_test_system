import json
import os
import re
import subprocess
from pathlib import Path
import csv

import click

CURRENT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))


def clean_directory(directory):
    try:
        subprocess.run(["rm", "-rf", directory])
        print(f"Все файлы и папки в {directory} удалены успешно.")
    except Exception as e:
        print(f"Произошла ошибка при удалении файлов и папок: {e}")


def create_directory_if_not_exist(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def delete_file(file_path):
    try:
        # Check if the file exists
        if os.path.exists(file_path):
            # Remove the file
            os.remove(file_path)
            print(f"File '{file_path}' deleted successfully.")
        else:
            print(f"File '{file_path}' does not exist.")

    except Exception as e:
        print(f"An error occurred: {e}")


def generate_solver_script(
        solver_bin, solver_params, tasks,
        solver_log_dir_stderr, solver_log_dir_stdout, script_dir,
        experiment_name):
    scripts_path = []
    for i, (task_name, task_path) in enumerate(tasks):
        script = script_dir / f"{experiment_name}_{task_name}.sh"
        with open(script, "w") as file:
            file.write("#!/bin/bash -i \n")
            file.write("source activate slurm_test_system \n")
            if isinstance(solver_params, list):
                solver_params = ' '.join(solver_params)
            file.write(
                f"/usr/bin/time -v {solver_bin} {task_path} {solver_params} > {solver_log_dir_stdout / task_name} 2> {solver_log_dir_stderr / task_name}")
        scripts_path.append(script)
    return scripts_path

def generate_interleave_v2_script(
        solver_bin, solver_params, tasks,
        solver_log_dir_stderr, solver_log_dir_stdout, script_dir,
        experiment_name):
    scripts_path = []
    for i, (task_name, task_path) in enumerate(tasks):
        script = script_dir / f"{experiment_name}_{task_name}.sh"
        with open(script, "w") as file:
            file.write("#!/bin/bash -i \n")
            file.write("source activate slurm_test_system \n")
            if isinstance(solver_params, list):
                solver_params = ' '.join(solver_params)
            file.write(
                f"/usr/bin/time -v {solver_bin} {task_path} {solver_params} --output  {solver_log_dir_stdout / (task_name + '_output.txt')}  > {solver_log_dir_stdout / task_name} 2> {solver_log_dir_stderr / task_name}")
        scripts_path.append(script)
    return scripts_path


def run_scripts(scripts, time_limit_s, slurm_log, cpus=1, mem=None, node="orthrus-2"):
    hours = time_limit_s // 60 // 60
    minutes = (time_limit_s - hours * 60 * 60) // 60
    seconds = time_limit_s - hours * 60 * 60 - minutes * 60
    jobs = []
    mem = mem if mem is not None else defaultdict(lambda : 16)

    for scripts_path in scripts:
        file_name = scripts_path.stem.split('_')[-1]
        command = ["sbatch", "-p", "as", f"--cpus-per-task={cpus}",
                   f"--mem={mem[file_name]}G", f"--time={hours}:{minutes}:{seconds}",
                   "-w", f"{node}", "--qos=high_cpu", "--qos=high_mem", "--qos=unlim_cpu",
                   f"--output={slurm_log}/slurm-%j.txt", f"--error={slurm_log}/slurm-%j.txt",
                   scripts_path]

        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)

        pattern = re.compile(r'Submitted batch job (\d+)')
        match = pattern.search(result.stdout)

        if match:
            job_id = match.group(1)
        else:
            raise Exception("Job ID not found in the output.")
        jobs.append((job_id, scripts_path, cpus, mem[file_name], time_limit_s))
    return jobs


def print_log(jobs_log, log_prefix):
    log = log_prefix
    with open(log, "w") as log_file:
        log_file.write(f"job_id:file_name:cpus:mem:seconds\n")

        for job_id, file_name, cpus, mem, seconds in jobs_log:
            log_file.write(f"{job_id}:{file_name}:{cpus}:{mem}:{seconds}\n")


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


def read_json_from_file(experiment_path):
    with open(experiment_path, 'r') as json_file:
        return json.load(json_file)


def get_solver_bin(experiment, solvers):
    if experiment['solver'] not in solvers:
        raise Exception(f"Unknown solver {experiment['solver']}")

    if not os.path.exists(solvers[experiment['solver']]):
        raise Exception(f"Can't find solver bin: {solvers[experiment['solver']]}")
    return solvers[experiment['solver']]


def prepare_directories(experiment):
    solver_log_dir = Path(experiment["log_dir"]) / experiment["name"]
    solver_log_dir_stderr = solver_log_dir / "stderr"
    solver_log_dir_stdout = solver_log_dir / "stdout"
    script_dir = Path(experiment["scripts_dir"]) / experiment["name"]
    jobid_dir = Path(experiment['jobid_dir'])
    slurm_log = Path(experiment['slurm_log']) / experiment["name"]


    create_directory_if_not_exist(solver_log_dir_stderr)
    create_directory_if_not_exist(solver_log_dir_stdout)
    create_directory_if_not_exist(script_dir)
    create_directory_if_not_exist(jobid_dir)
    create_directory_if_not_exist(slurm_log)
    return solver_log_dir_stderr, solver_log_dir_stdout, script_dir, jobid_dir, slurm_log


def get_experiment_tasks(tasks_path):
    if not os.path.exists(tasks_path):
        raise Exception(f"Can't find tasks path: {tasks_path}")
    tasks = []
    with open(tasks_path, 'r') as tasks_file:
        for task_path in tasks_file:
            task_path_ = Path(task_path.strip())
            if not os.path.exists(task_path_):
                raise Exception(f"Can't find task path: {task_path_}")
            tasks.append((task_path_.stem, task_path_))
    return tasks


def get_experiment_mems(mems_file_path):
    if not os.path.exists(mems_file_path):
        raise Exception(f"Can't find mems path: {mems_file_path}")
    mems = {}
    with open(mems_file_path, newline='', encoding='utf-8') as csvfile:
        # Создаем объект для чтения CSV
        reader = csv.DictReader(csvfile)

        # Читаем каждую строку из CSV файла
        for row in reader:
            # Добавляем строки в список data
            mems[row['task']] = row['mem']
    return mems


@click.command()
@click.argument('experiment_path', required=True, type=click.Path(exists=True))
@click.option("--rerun", type=bool,
              help="rerun experiment", default=False)
@click.option("--time-limit-s", type=int, default=5100)
def run_experiments(experiment_path: str, rerun: bool, time_limit_s: int):
    experiment_path = Path(experiment_path)
    solvers_path = CURRENT_DIR / "solvers_path.json"

    experiment = read_json_from_file(experiment_path)['experiment']
    solvers = read_json_from_file(solvers_path)['solvers_path']
    solver_bin = get_solver_bin(experiment, solvers)

    solver_log_dir_stderr, solver_log_dir_stdout, script_dir, jobid_dir, slurm_log = prepare_directories(experiment)
    jobid_log_file = jobid_dir / f"{experiment['name']}.csv"

    if rerun:
        if os.path.exists(jobid_log_file):
            cancel_jobs(jobid_log_file)
        clean_directory(solver_log_dir_stderr)
        clean_directory(solver_log_dir_stdout)
        clean_directory(slurm_log)
        clean_directory(script_dir)
        delete_file(jobid_log_file)
        solver_log_dir_stderr, solver_log_dir_stdout, script_dir, jobid_dir, slurm_log = prepare_directories(experiment)
        print("All files have been deleted")
    else:
        if os.path.exists(jobid_log_file):
            raise Exception("duplicate conf")


    tasks = get_experiment_tasks(experiment['tasks_path'])
    solver_params = experiment['param']
    experiment_name = experiment['name']
    mems = get_experiment_mems(experiment['mem'])

    if "interleave_v2" in experiment['solver']:
        scripts_path = generate_interleave_v2_script(
            solver_bin, solver_params, tasks,
            solver_log_dir_stderr, solver_log_dir_stdout, script_dir,
            experiment_name)
    else:
        scripts_path = generate_solver_script(
            solver_bin, solver_params, tasks,
            solver_log_dir_stderr, solver_log_dir_stdout, script_dir,
            experiment_name)

    jobs_log = run_scripts(scripts_path, time_limit_s, slurm_log,
                           cpus=experiment['cpus'], mem=mems,
                           node=experiment['node'])

    print_log(jobs_log, jobid_log_file)
    print("scripts run")


if __name__ == "__main__":
    run_experiments()
