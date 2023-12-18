import os
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from time import sleep

import click

CURRENT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))


def read_log(log_path: Path):
    log = dict()
    with open(log_path, "r") as log_file:
        for line in log_file:
            if "job_id" in line:
                continue
            job_id, file_name, cpus, mem, hours = line.split(":")

            file_name = file_name[:file_name.index('.')]
            # TODO костыль

            log[job_id] = {"job_id": job_id,
                        "file_name": file_name,
                        "cpus": cpus,
                        "mem": mem,
                        "hours": hours}
    return log


def is_complete(all_kissat_task, all_multithreading_solver_task,
                processed_kissat_task, processed_multithreading_solver):
    return ((len(set(all_kissat_task) - set(processed_kissat_task)) == 0) and
            (len(set(all_multithreading_solver_task) - set(processed_multithreading_solver)) == 0))


def check_job_status(job_id):
    try:
        # Run scontrol to get job information
        result = subprocess.run(['scontrol', 'show', 'job', str(job_id)], capture_output=True, text=True, check=True)

        # Check the output for the job status
        output_lines = result.stdout.split('\n')
        for line in output_lines:
            for batch in line.strip().split():
                if batch.startswith('JobState='):
                    job_state = batch.split('=')[1].strip()
                    return job_state
        return None
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None

def parse_process_time_line(line):
    words = line.split()
    return int(float(words[-2]))

def get_kissat_task_info(task, logs, time_limit_s):
    file_name = task['file_name']
    log_file_path = logs / f"{file_name}.cnf.log"
    task_copy = deepcopy(task)
    status = "UNDEFINED"
    process_time = None
    with open(log_file_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if "SATISFIABLE" in line:
                status = "SATISFIABLE"
            if "UNSATISFIABLE" in line:
                status = "UNSATISFIABLE"
            if "process-time" in line:
                process_time = parse_process_time_line(line)
            if not line:
                if process_time is None:
                    raise Exception("process_time is None")
                if status == "UNDEFINED" and int(process_time) + 100 < time_limit_s :
                    raise Exception("UNDEFINED is unexpected")
                task_copy["status"] = status
                task_copy["process_time"] = process_time
                return task_copy

def parse_cpu_time_line(line):
    words = line.split()
    return int(float(words[-1]))

def get_multithreading_solver_task_info(task, logs, time_limit_s):
    file_name = task['file_name']
    log_file_path = logs / f"{file_name}.cnf.log"
    task_copy = deepcopy(task)
    status = "UNDEFINED"
    process_time = None
    with open(log_file_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if "SAT" in line:
                status = "SATISFIABLE"
            if "UNSAT" in line:
                status = "UNSATISFIABLE"
            if "CPU time" in line:
                process_time = parse_cpu_time_line(line)
            if not line:
                if process_time is None:
                    task_copy["status"] = "ERROR process_time is None"
                    task_copy["process_time"] = 0
                    return task_copy
                if status == "UNDEFINED" and int(process_time) + 100 < time_limit_s:
                    raise Exception("UNDEFINED is unexpected")
                task_copy["status"] = status
                task_copy["process_time"] = process_time
                return task_copy


def processed(all_task, processed_task, logs, time_limit_s, csv_file, get_info):
    unprocessed_task = set(all_task.keys()) - processed_task
    complete_task = set()
    for task_id in unprocessed_task:
        status = check_job_status(task_id)
        if status is None:
            task_info = get_info(all_task[task_id], logs, time_limit_s)
            with open(csv_file, "a") as csv:
                csv.write(f"{task_info['job_id']},{task_info['file_name']},{task_info['status']},{task_info['process_time']}\n")
            complete_task.add(task_id)
            print(f"task {task_id} was complete")
    return complete_task


@click.command()
@click.option('--kissat-experiments-log', required=True, type=click.Path(exists=True))
@click.option('--multithreading-solver-log', required=True, type=click.Path(exists=True))
@click.option('--logs', type=click.Path(exists=False), default="logs",
              help='logs dir')
@click.option('--time-limit-s', type=int, default=5000, help='time limit for solver')
@click.option('--kissat-csv-file', type=click.Path(exists=False), default="kissat.csv")
@click.option('--multithreading-solver-csv-file', type=click.Path(exists=False), default="multithreading_solver.csv")
def run_demon(kissat_experiments_log: str, multithreading_solver_log: str, logs: str,
              time_limit_s, kissat_csv_file, multithreading_solver_csv_file):
    kissat_experiments_log = Path(kissat_experiments_log)
    multithreading_solver_log = Path(multithreading_solver_log)
    kissat_csv_file = Path(kissat_csv_file)
    multithreading_solver_csv_file = Path(multithreading_solver_csv_file)
    logs = Path(logs)

    all_kissat_task = read_log(kissat_experiments_log)
    all_multithreading_solver_task = read_log(multithreading_solver_log)

    logs = CURRENT_DIR / logs

    processed_kissat_task = set()
    processed_multithreading_solver = set()

    with open(kissat_csv_file, 'w') as csv_file:
        csv_file.write("job_id,file_name,status,time\n")

    with open(multithreading_solver_csv_file, 'w') as csv_file:
        csv_file.write("job_id,file_name,status,time\n")

    while True:

        processed_kissat_task.update(
            processed(all_kissat_task,
                             processed_kissat_task, logs, time_limit_s, kissat_csv_file, get_kissat_task_info))
        processed_multithreading_solver.update(
            processed(all_multithreading_solver_task,
                processed_multithreading_solver, logs, time_limit_s, multithreading_solver_csv_file,
                      get_multithreading_solver_task_info))

        if is_complete(all_kissat_task.keys(), all_multithreading_solver_task.keys(),
                          processed_kissat_task, processed_multithreading_solver):
            break
        sleep(100)

if __name__ == "__main__":
    run_demon()