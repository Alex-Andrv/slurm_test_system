import json
import os
import re
import subprocess
from copy import deepcopy
from pathlib import Path
from time import sleep
from pysat.formula import CNF
from pysat.solvers import Minisat22

import click

CURRENT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))


def read_log(log_path: Path):
    log = dict()
    with open(log_path, "r") as log_file:
        for line in log_file:
            if "job_id" in line:
                continue
            job_id, file_name, cpus, mem, seconds = line.split(":")

            file_name = Path(file_name).stem
            # if (job_id in log):
            #     print("gg")
            log[job_id] = {"job_id": job_id,
                           "file_name": file_name,
                           "cpus": cpus,
                           "mem": mem,
                           "seconds": seconds}
    return log


def is_complete(all_task, processed_task):
    return len(set(all_task)) == len(set(processed_task))


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


# c process-time:                     1h 23m 20s            4999.99 seconds
# def parse_process_kissat_time_line(line):
#     words = line.split()
#     return int(float(words[-2]))


def get_cadical_info(task, experiment, csv_file):
    file_name = task['file_name'][len(experiment['name']) + 1:]
    out_log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stdout' / file_name
    err_log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stderr' / file_name
    task_copy = deepcopy(task)
    status = "UNDEFINED"
    process_time = None
    max_ram = 0
    with open(err_log_file_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if "Maximum resident set size" in line:
                max_ram = float(line.split()[-1]) / 1024 / 1024
            if not line:
                break
    with open(out_log_file_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if "SATISFIABLE" in line:
                status = "SATISFIABLE"
            if "UNSATISFIABLE" in line:
                status = "UNSATISFIABLE"
            if "c total process time since initialization:" in line:
                process_time = parse_process_time_line(line)
            if "out_of_memory" in line:
                status = "out_of_memory"
            if not line:
                if process_time is None:
                    status = "OOM"
                    process_time = task['seconds']
                task_copy["status"] = status
                task_copy["process_time"] = process_time
                break
    with open(csv_file, "a") as csv:
        csv.write(
            f"{task_copy['job_id']},{task_copy['file_name']},{task_copy['status']},{task_copy['process_time']},{max_ram}\n")
    return task_copy


def get_kissat_info(task, experiment, csv_file):
    file_name = task['file_name'][len(experiment['name']) + 1:]
    log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stdout' / file_name
    task_copy = deepcopy(task)
    status = "UNDEFINED"
    process_time = None
    with open(log_file_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if "s SATISFIABLE" in line:
                status = "SATISFIABLE"
            if "s UNSATISFIABLE" in line:
                status = "UNSATISFIABLE"
            if "c process-time:" in line:
                process_time = parse_process_time_line(line)
            if "out_of_memory" in line:
                status = "out_of_memory"
            if not line:
                if process_time is None:
                    status = "OOM"
                    process_time = task['seconds']
                task_copy["status"] = status
                task_copy["process_time"] = process_time
                break
    with open(csv_file, "a") as csv:
        csv.write(
            f"{task_copy['job_id']},{task_copy['file_name']},{task_copy['status']},{task_copy['process_time']}\n")
    return task_copy


def get_sbva_cadical_info(task, experiment, csv_file):
    file_name = task['file_name'][len(experiment['name']) + 1:]
    log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stdout' / file_name
    task_copy = deepcopy(task)
    status = "UNDEFINED"
    process_time = None

    slurm_file = f"./slurm-{task['job_id']}.out"
    with open(slurm_file, 'r') as slurm:
        for line in slurm:
            if "real" in line:
                process_time = time_string_to_seconds(line.split()[1])
            if "error" in line:
                if "out-of-memory handler" in line:
                    status = "OUT_OF_MEMORY"
                    process_time = 5100
                elif "DUE TO TIME LIMIT" in line:
                    status = "SLURM_TIME_LIMIT"
                    process_time = 5100
                else:
                    status = line
                    process_time = 5100
                break

    if process_time is None:
        raise Exception("some error")

    with open(log_file_path, 'r') as log_file:
        while True:
            line = log_file.readline()
            if "SATISFIABLE" in line:
                status = "SATISFIABLE"
            if "UNSATISFIABLE" in line:
                status = "UNSATISFIABLE"
            if not line:
                if process_time is None:
                    status = "OOM"
                    process_time = task['seconds']
                task_copy["status"] = status
                task_copy["process_time"] = process_time
                break
    with open(csv_file, "a") as csv:
        csv.write(
            f"{task_copy['job_id']},{task_copy['file_name']},{task_copy['status']},{task_copy['process_time']}\n")
    return task_copy


def time_string_to_seconds(time_string):
    match = re.match(r'(\d+)m(\d+\.\d+)s', time_string)
    if match:
        minutes = float(match.group(1))
        seconds = float(match.group(2))
        total_seconds = minutes * 60 + seconds
        return total_seconds
    else:
        return None


def read_model(model):
    with open(model, 'r') as model:
        first_line = model.readline()
        assert ("s SATISFIABLE" in first_line), "Invalide format"
        second_line = model.readline().split()
        second_line = list(map(int, second_line[1:-1]))
        return second_line

def parse_slurm_log(slurm_file):
    status = "UNDEFINED"
    process_time = None
    with open(slurm_file, 'r') as slurm:
        for line in slurm:
            if "real" in line:
                process_time = time_string_to_seconds(line.split()[1])
            if "error" in line:
                if "out-of-memory handler" in line:
                    status = "OUT_OF_MEMORY"
                    process_time = 5100
                elif "DUE TO TIME LIMIT" in line:
                    status = "SLURM_TIME_LIMIT"
                    process_time = 5100
                else:
                    status = line
                    process_time = 5100
                break
    return status, process_time


def get_interleave_info(task, experiment, csv_file):
    file_name = task['file_name'][len(experiment['name']) + 1:]
    err_log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stderr' / file_name
    out_log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stdout' / file_name
    slurm_file = Path(experiment['slurm_log']) / experiment['name'] / f"./slurm-{task['job_id']}.txt"
    task_copy = deepcopy(task)

    total_derive_0 = 0
    total_derive = 0
    units_value = 0
    binary_value = 0
    other = 0

    max_ram = 0

    status, process_time = parse_slurm_log(slurm_file)

    with open(err_log_file_path, 'r') as log_file:
        for line in log_file:
            if "Message:  " in line:
                if "Unexpected SAT" in line:
                    status = "SAT"
                elif "Found strong backdoor" in line:
                    status = "Found strong"
                else:
                    status = f"ERROR: {line}"
                    process_time = 5100
                    raise ValueError(status)
            if "User time (seconds):" in line:
                process_time = int(float(line.split()[-1]))
            if " SAT" in line:
                status = f"SAT"
            if "UNSAT" in line:
                status = f"UNSAT"
            if "Maximum resident set size" in line:
                max_ram = int(line.split()[-1]) / 1024 / 1024
            if "So far derived" in line:
                regex_pattern = r"\((\d+) units, (\d+) binary, (\d+) other\)"
                match = re.search(regex_pattern, line)
                if match:
                    units_value = match.group(1)
                    binary_value = match.group(2)
                    other = match.group(3)
                else:
                    raise Exception(f"Can't parse So far derived")
            if "Derived 0 new clauses (0 units, 0 binary, 0 other)" in line:
                total_derive_0 += 1
            if ("Derived" in line) and ("new" in line):
                total_derive += 1


    task_copy['units_value'] = units_value
    task_copy['binary_value'] = binary_value
    task_copy['status'] = status
    task_copy['process_time'] = process_time
    task_copy['other_value'] = other
    task_copy['total_derive_0'] = total_derive_0
    task_copy['all_derive'] = total_derive

    with open(csv_file, "a") as csv:
        csv.write(
            f"{task_copy['job_id']},{task_copy['file_name']},{task_copy['status']},{task_copy['process_time']},"
            f"{task_copy['units_value']},{task_copy['binary_value']},{task_copy['other_value']},{task_copy['total_derive_0']},{task_copy['all_derive']},{task_copy['total_derive_0']/task_copy['all_derive'] if task_copy['all_derive'] != 0 else 0},{max_ram}\n")
    return task_copy

def get_interleave_v2_info(task, experiment, csv_file):
    file_name = task['file_name'][len(experiment['name']) + 1:]
    err_log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stderr' / file_name
    out_log_file_path = Path(experiment['log_dir']) / experiment['name'] / 'stdout' / file_name
    slurm_file = Path(experiment['slurm_log']) / experiment['name'] /  f"./slurm-{task['job_id']}.txt"
    task_copy = deepcopy(task)


    total_derive_0 = 0
    total_derive = 0
    units_value = 0
    binary_value = 0
    other = 0

    max_ram = 0

    status, process_time = parse_slurm_log(slurm_file)

    with open(err_log_file_path, 'r') as log_file:
        for line in log_file:
            if "Message:  " in line:
                status = f"ERROR: {line}"
                process_time = 5100
                break
            if "User time (seconds):" in line:
                process_time = int(float(line.split()[-1]))
            if " SAT" in line:
                status = f"SAT"
            if "UNSAT" in line:
                if process_time == 5100:
                    raise Exception(f"JOB_ID = {task['job_id']}")
            # if "Found strong backdoor" in line:
            #     status = f"Found strong backdoor"
            #     break
            if "Maximum resident set size" in line:
                max_ram = int(line.split()[-1]) / 1024 / 1024
            if "So far derived" in line:
                regex_pattern = r"\((\d+) units, (\d+) binary, (\d+) ternary, (\d+) other\)"
                match = re.search(regex_pattern, line)
                if match:
                    units_value = match.group(1)
                    binary_value = match.group(2)
                    other = match.group(3)
                else:
                    raise Exception(f"Can't parse So far derived")
            if "Derived 0 new clauses (0 units, 0 binary, 0 other)" in line:
                total_derive_0 += 1
            if ("Derived" in line) and ("new" in line):
                total_derive += 1



    with open(out_log_file_path, 'r') as log_file:
        for line in log_file:
            if "UNSAT" in line:
                if status == "UNSAT" or status == "UNDEFINED":
                    status = "UNSAT"
                else:
                    raise ValueError(f"unexpected UNSAT, {status}")
            elif "SAT" in line:
                if status == "SAT" or status == "UNDEFINED":
                    status = "SAT"
                else:
                    raise ValueError("unexpected SAT")

    if status == "SAT":
        print("check")
        model = Path(experiment['log_dir']) / experiment['name'] / 'stdout' / f"{file_name}_output.txt"
        units = read_model(model)

        problem = Path("/nfs/home/aandreev/slurm_test_system/sta_comp_2023") / f"{file_name}.cnf"

        cnf = CNF(from_file=problem).clauses
        for unit in units:
            cnf.append([unit])
        # if file_name
        with Minisat22(bootstrap_with=cnf) as m:
            res = m.solve()
            if res == False:
                raise ValueError("Must be SAT")
            else:
                print("successful SAT")

    task_copy['units_value'] = units_value
    task_copy['binary_value'] = binary_value
    task_copy['status'] = status
    task_copy['process_time'] = process_time
    task_copy['other_value'] = other
    task_copy['total_derive_0'] = total_derive_0
    task_copy['all_derive'] = total_derive

    with open(csv_file, "a") as csv:
        csv.write(
            f"{task_copy['job_id']},{task_copy['file_name']},{task_copy['status']},{task_copy['process_time']},"
            f"{task_copy['units_value']},{task_copy['binary_value']},{task_copy['other_value']},{task_copy['total_derive_0']},{task_copy['all_derive']},{task_copy['total_derive_0']/task_copy['all_derive'] if task_copy['all_derive'] != 0 else 0},{max_ram}\n")
    return task_copy



def processed(all_task, processed_task, experiment, csv_file, parse_log):
    unprocessed_task = set(all_task.keys()) - processed_task
    complete_task = set()
    for task_id in unprocessed_task:
        status = check_job_status(task_id)
        try:
            if status is None:
                task_info = parse_log(all_task[task_id], experiment, csv_file)
                complete_task.add(task_id)
                print(f"task {task_id} was complete")
        except FileNotFoundError as e:
            print(e)
    return complete_task

def read_json_from_file(experiment_path):
    with open(experiment_path, 'r') as json_file:
        return json.load(json_file)

def create_directory_if_not_exist(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def print_header(experiment, statistics):
    statistics_file = statistics / f"{experiment['name']}.csv"
    if "interleave" in experiment['solver']:
        with open(statistics_file, 'w') as csv_file:
            csv_file.write("job_id,file_name,status,time,new_unary,new_binary,other,total_derive_0,all_derive,percentage,max_mem_GB\n")
    elif "cadical" in experiment['solver']:
        with open(statistics_file, 'w') as csv_file:
            csv_file.write("job_id,file_name,status,time,max_mem_GB\n")
    elif "kissat" in experiment['solver']:
        with open(statistics_file, 'w') as csv_file:
            csv_file.write("job_id,file_name,status,time\n")
    else:
        raise Exception("Undefined solver")
    return statistics_file

@click.command()
@click.argument('experiments_path', nargs=-1, required=True, type=click.Path(exists=True))
def run_demon(experiments_path: str):
    statistics = CURRENT_DIR / "statistics"
    create_directory_if_not_exist(statistics)

    experiments = [read_json_from_file(exp)['experiment'] for exp in experiments_path]

    experiments_all_task = [read_log(Path(exp['jobid_dir']) / f"{exp['name']}.csv") for exp in experiments]

    experiments_statistics_files = [print_header(exp, statistics) for exp in experiments]
    all_processed_task = [set() for _ in range(len(experiments))]
    while True:
        for experiment, all_task, statistics_file, processed_task in zip(experiments, experiments_all_task, experiments_statistics_files, all_processed_task):
            solver = experiment['solver']
            if "sbva_interleave" in solver:
                processed_task.update(
                    processed(all_task,
                              processed_task, experiment,
                              statistics_file, get_interleave_info))
            elif "sbva_cadical" in solver:
                processed_task.update(
                    processed(all_task,
                              processed_task, experiment,
                              statistics_file, get_sbva_cadical_info))
            elif "cadical" in solver:
                processed_task.update(
                processed(all_task,
                          processed_task, experiment,
                          statistics_file, get_cadical_info))

            elif "interleave_v2" in solver:
                processed_task.update(
                    processed(all_task,
                              processed_task, experiment,
                              statistics_file, get_interleave_v2_info))
            elif "interleave" in solver:
                processed_task.update(
                    processed(all_task,
                              processed_task, experiment,
                              statistics_file, get_interleave_info))
            elif "kissat" in solver:
                processed_task.update(
                    processed(all_task,
                              processed_task, experiment,
                              statistics_file, get_kissat_info))
            else:
                raise Exception(f"Unsupported solver type {solver}")
        sleep(5)


if __name__ == "__main__":
    run_demon()
