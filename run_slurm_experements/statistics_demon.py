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

            file_name = file_name[:-23]
            # TODO костыль

            log[job_id] = {"job_id": job_id,
                           "file_name": file_name,
                           "cpus": cpus,
                           "mem": mem,
                           "hours": hours}
    return log


def is_complete(all_kissat_task, all_multithreading_solver_task, all_search_product_derive_task,
                processed_kissat_task, processed_multithreading_solver, processed_search_product_derive):
    return ((len(set(all_kissat_task) - set(processed_kissat_task)) == 0) and
            (len(set(all_multithreading_solver_task) - set(processed_multithreading_solver)) == 0) and
            (len(set(all_search_product_derive_task) - set(processed_search_product_derive)) == 0))


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


def get_kissat_task_info(task, logs, time_limit_s, csv_file):
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
            if "c total process time since initialization:" in line:
                process_time = parse_process_time_line(line)
            if not line:
                if process_time is None:
                    status = "out_of_memory"
                    print("out_of_memory")
                if status == "UNDEFINED" and int(process_time) + 100 < time_limit_s:
                    raise Exception("UNDEFINED is unexpected")
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

def get_search_product_derive_task_info(task, logs, time_limit_s, csv_file):
    file_name = task['file_name']
    log_file_path = logs / f"{file_name}.cnf.log"
    slurm_file = f"./slurm-{task['job_id']}.out"
    task_copy = deepcopy(task)
    status = "UNDEFINED/UNSAT"
    process_time = None
    units_value = None
    binary_value = None
    total_derive_0 = 0
    total_derive = 0
    # with open(log_file_path, 'r') as log_file:
    #     for line in log_file:
    #         if line.startswith('All done in'):
    #             process_time = float(line.split()[3])
    with open(slurm_file, 'r') as log_file:
        for line in log_file:
            if ' SAT' in line:
                status = "SAT"
            if "Hard task" in line:
                status = "HARD"
            if "CANCELLED" in line:
                status = "SLURM_TIME_LIMIT"
            if "out-of-memory" in line:
                status = "OUT_OF_MEMORY"
            if "Found strong backdoor" in line:
                status = "Found strong backdoor"
            if "real" in line:
                process_time = time_string_to_seconds(line.split()[1])
            if "So far derived" in line:
                regex_pattern = r"\((\d+) units, (\d+) binary, (\d+) other\)"

                match = re.search(regex_pattern, line)

                if match:
                    units_value = match.group(1)
                    binary_value = match.group(2)
                    other = match.group(3)
                else:
                    print("error")
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
            f"{task_copy['units_value']},{task_copy['binary_value']},{task_copy['other_value']},{task_copy['total_derive_0']},{task_copy['all_derive']},{task_copy['total_derive_0']/task_copy['all_derive']}\n")
    return task_copy


def parse_cpu_time_line(line):
    words = line.split()
    return int(float(words[-1]))


def get_start_parameters(producer_name):
    parts = producer_name.split("-")[2:]
    params = {key: value for key, value in [part.split(":") for part in parts]}
    return params


def safe_mean(array_value):
    if len(array_value) == 0:
        return 0
    return sum(array_value) / len(array_value)


def get_avg_ro(experiments):
    ro_s = []

    for dir in os.listdir(experiments):
        backdoor_info = experiments / dir / "backdoor.txt"
        if os.path.exists(backdoor_info):
            with open(backdoor_info, 'r') as backdoor_info_file:
                for line in backdoor_info_file:
                    match = re.search(r'rho = ([0-9.]+)', line)
                    ro_s.append(float(match.group(1)))
    return safe_mean(ro_s), len(ro_s)


def get_derived(experiments):
    cnt_unit = []
    cnt_binary = []

    for dir in os.listdir(experiments):
        derived = experiments / dir / "derived.txt"
        if os.path.exists(derived):
            with open(derived, 'r') as derived_file:
                lines = [line.split() for line in derived_file.readlines()]
                for line in lines:
                    if line[-1] != "0":
                        raise ValueError("cnt mast end with 0")
                units = sum([1 for line in lines if len(line) == 2])
                binary = sum([1 for line in lines if len(line) == 3])
                if units + binary != len(lines):
                    raise ValueError("units + binary != len(lines)")
                cnt_binary.append(binary)
                cnt_unit.append(units)
    return safe_mean(cnt_unit), safe_mean(cnt_binary), len(cnt_unit)


def get_new_info(experiments):
    cnt_unit = []
    cnt_binary = []

    for dir in os.listdir(experiments):
        statistics = experiments / dir / "statistics"
        if os.path.exists(statistics):
            with open(statistics, 'r') as statistics_file:
                units = 0
                binary = 0
                for line in statistics_file:
                    if line.startswith("real deriving unique new_units where"):
                        match = re.search(r'([0-9]+)', line)
                        units += int(match.group(1))
                    if line.startswith("real deriving unique new_binary where"):
                        match = re.search(r'([0-9]+)', line)
                        binary += int(match.group(1))
                    if line.startswith("real deriving unique new_ternary where"):
                        match = re.search(r'([0-9]+)', line)
                        if int(match.group(1)) != 0:
                            raise ValueError("Don't expect ternary clause")
                    if line.startswith("real deriving unique new_large where"):
                        match = re.search(r'([0-9]+)', line)
                        if int(match.group(1)) != 0:
                            raise ValueError("Don't expect large clause")
                cnt_unit.append(units)
                cnt_binary.append(binary)
    return safe_mean(cnt_unit), safe_mean(cnt_binary), len(cnt_unit)


def get_spend_time(experiments):
    cnt_time = []

    for dir in os.listdir(experiments):
        statistics = experiments / dir / "statistics"
        if os.path.exists(statistics):
            with open(statistics, 'r') as statistics_file:
                for line in statistics_file:
                    if line.startswith("calculation time, seconds:"):
                        match = re.search(r'([0-9.]+)', line)
                        cnt_time.append(float(match.group(1)))
                        break

    return safe_mean(cnt_time), max(cnt_time, default=0), min(cnt_time, default=0)


def get_producer_info(task_info):
    root_dir = Path('/mnt/tank/scratch/aandreev/log')

    info_dir = root_dir / f"{task_info['file_name'].split('_')[-1]}.cnf"
    all_entries = os.listdir(info_dir)
    try:
        for i, entry in enumerate(all_entries):
            if entry.startswith("clause-producer"):
                params = get_start_parameters(entry)
                experiments = info_dir / entry / "experement"
                avg_ro, cnt_backdoors = get_avg_ro(experiments)
                avg_unit, avg_binary, cnt_derived_experiments = get_derived(experiments)
                avg_new_unit, avg_new_binary, cnt_new_info_experiments = get_new_info(experiments)
                avg_time, max_time, min_time = get_spend_time(experiments)
                task_info[f"bp{int(params['instance']) + 1}_avg_ro"] = avg_ro
                task_info[f"bp{int(params['instance']) + 1}_cnt_backdoors"] = cnt_backdoors
                task_info[f"bp{int(params['instance']) + 1}_avg_unit"] = avg_unit
                task_info[f"bp{int(params['instance']) + 1}_avg_binary"] = avg_binary
                task_info[f"bp{int(params['instance']) + 1}_cnt_derived_experiments"] = cnt_derived_experiments
                task_info[f"bp{int(params['instance']) + 1}_avg_new_unit"] = avg_new_unit
                task_info[f"bp{int(params['instance']) + 1}_avg_new_binary"] = avg_new_binary
                task_info[f"bp{int(params['instance']) + 1}_avg_time"] = avg_time
                task_info[f"bp{int(params['instance']) + 1}_max_time"] = max_time
                task_info[f"bp{int(params['instance']) + 1}_min_time"] = min_time
                task_info['cnt_instance'] = max(int(params['instance']) + 1, task_info.get('cnt_instance', 0))

    except Exception as e:
        print(f"some bug in {task_info['file_name']}")
        print(f"Error {e}")

    return task_info


def get_multithreading_solver_task_info(task, logs, time_limit_s, csv_file):
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
                    break
                if status == "UNDEFINED" and int(process_time) + 100 < time_limit_s:
                    raise Exception("UNDEFINED is unexpected")
                task_copy["status"] = status
                task_copy["process_time"] = process_time
                break

    task_copy = get_producer_info(task_copy)
    with open(csv_file, "a") as csv:
        line = f"{task_copy['job_id']},{task_copy['file_name']},{task_copy['status']},{task_copy['process_time']}"
        for i in range(6):
            line += (f","
                     f"{task_copy.get(f'bp{i + 1}_avg_ro')},"
                     f"{task_copy.get(f'bp{i + 1}_cnt_backdoors')},"
                     f"{task_copy.get(f'bp{i + 1}_cnt_derived_experiments')},"
                     f"{task_copy.get(f'bp{i + 1}_avg_unit')},"
                     f"{task_copy.get(f'bp{i + 1}_avg_binary')},"
                     f"{task_copy.get(f'bp{i + 1}_avg_new_unit')},"
                     f"{task_copy.get(f'bp{i + 1}_avg_new_binary')},"
                     f"{task_copy.get(f'bp{i + 1}_avg_time')},"
                     f"{task_copy.get(f'bp{i + 1}_max_time')},"
                     f"{task_copy.get(f'bp{i + 1}_min_time')}")
        csv.write(line + "\n")

    return task_copy


def processed(all_task, processed_task, logs, time_limit_s, csv_file, get_info):
    unprocessed_task = set(all_task.keys()) - processed_task
    complete_task = set()
    for task_id in unprocessed_task:
        status = check_job_status(task_id)
        try:
            if status is None:
                task_info = get_info(all_task[task_id], logs, time_limit_s, csv_file)
                complete_task.add(task_id)
                print(f"task {task_id} was complete")
        except FileNotFoundError as e:
            print(e)
    return complete_task


@click.command()
@click.option('--kissat-experiments-log', default="kissat.log", type=click.Path(exists=False))
@click.option('--multithreading-solver-log', default="multithreading_solver.log", type=click.Path(exists=False))
@click.option('--search-product-derive-log', default="search_product_derive_12.0.log", type=click.Path(exists=True))
@click.option('--logs', type=click.Path(exists=False), default="logs",
              help='logs dir')
@click.option('--time-limit-s', type=int, default=5000, help='time limit for solver')
@click.option('--kissat-csv-file', type=click.Path(exists=False), default="kissat.csv")
@click.option('--multithreading-solver-csv-file', type=click.Path(exists=False), default="multithreading_solver.csv")
@click.option('--search-product-derive-csv-file', type=click.Path(exists=False), default="search-product-derive_12.0.csv")
def run_demon(kissat_experiments_log: str, multithreading_solver_log: str, search_product_derive_log, logs: str,
              time_limit_s, kissat_csv_file, multithreading_solver_csv_file, search_product_derive_csv_file):
    kissat_experiments_log = Path(kissat_experiments_log)
    multithreading_solver_log = Path(multithreading_solver_log)
    search_product_derive_log = Path(search_product_derive_log)
    search_product_derive_log_2 = Path("search_product_derive_14.0.log")

    kissat_csv_file = Path(kissat_csv_file)
    multithreading_solver_csv_file = Path(multithreading_solver_csv_file)
    search_product_derive_csv_file = Path(search_product_derive_csv_file)
    search_product_derive_csv_file_2 = Path("search-product-derive_14.0.csv")
    logs = Path(logs)

    all_kissat_task = read_log(kissat_experiments_log)
    # all_multithreading_solver_task = read_log(multithreading_solver_log)
    # all_kissat_task = dict()
    all_multithreading_solver_task = dict()
    all_search_product_derive_task = read_log(search_product_derive_log)
    all_search_product_derive_task_2 = read_log(search_product_derive_log_2)

    logs = CURRENT_DIR / logs

    processed_kissat_task = set()
    processed_multithreading_solver = set()
    processed_search_product_derive = set()

    with open(kissat_csv_file, 'w') as csv_file:
        csv_file.write("job_id,file_name,status,time\n")

    with open(multithreading_solver_csv_file, 'w') as csv_file:
        line = "job_id,file_name,status,time"
        for i in range(6):
            line += (f","
                     f"bp{i + 1}_avg_ro,"
                     f"bp{i + 1}_cnt_backdoors,"
                     f"bp{i + 1}_cnt_derived_experiments,"
                     f"bp{i + 1}_avg_unit,"
                     f"bp{i + 1}_avg_binary,"
                     f"bp{i + 1}_avg_new_unit,"
                     f"bp{i + 1}_avg_new_binary,"
                     f"bp{i + 1}_avg_time,"
                     f"bp{i + 1}_max_time,"
                     f"bp{i + 1}_min_time")
        csv_file.write(f"{line}\n")

    with open(search_product_derive_csv_file, 'w') as csv_file:
        csv_file.write("job_id,file_name,status,time,new_unary,new_binary,other,total_derive_0,all_derive,percentage\n")

    with open(search_product_derive_csv_file_2, 'w') as csv_file:
        csv_file.write("job_id,file_name,status,time,new_unary,new_binary,other,total_derive_0,all_derive,percentage\n")

    while True:
        #
        # processed_kissat_task.update(
        #     processed(all_kissat_task,
        #                      processed_kissat_task, logs, time_limit_s, kissat_csv_file, get_kissat_task_info))
        # # processed_multithreading_solver.update(
        #     processed(all_multithreading_solver_task,
        #               processed_multithreading_solver, logs, time_limit_s, multithreading_solver_csv_file,
        #               get_multithreading_solver_task_info))

        processed_search_product_derive.update(
            processed(all_search_product_derive_task,
                      processed_search_product_derive, logs,
                      time_limit_s, search_product_derive_csv_file,
                      get_search_product_derive_task_info))

        processed_search_product_derive.update(
            processed(all_search_product_derive_task_2,
                      processed_search_product_derive, logs,
                      time_limit_s, search_product_derive_csv_file_2,
                      get_search_product_derive_task_info))

        if is_complete(all_kissat_task.keys(), all_multithreading_solver_task.keys(),
                       all_search_product_derive_task.keys(),
                       processed_kissat_task, processed_multithreading_solver, processed_search_product_derive):
            break
        sleep(100)


if __name__ == "__main__":
    run_demon()
