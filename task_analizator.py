import os
from pathlib import Path

import click
from tqdm import tqdm

CURRENT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))


def analyze(task_dir):
    task = dict()
    for file_name in tqdm(os.listdir(task_dir)):
        with open(task_dir/file_name, 'r') as cnf_file:
            while True:
                line = cnf_file.readline()
                if line.startswith("p cnf"):
                    _, _, vars, clauses = line.split()
                    if (int(vars) < 10_000) and (int(clauses) < 100_000):
                        task[file_name[:-4]] = {'vars': vars, "clauses": clauses}
                    break
    return task



@click.command()
@click.option('--task-dir', type=str, default="sta_comp_2023",
              help='sat comp 2023 task output dir')
@click.option('--statistics-file', type=str, default="statistics.txt")
def download(task_dir: str, statistics_file):
    task_dir = CURRENT_DIR / Path(task_dir)

    task = analyze(task_dir)

    with open(statistics_file, 'w') as out_file:
        out_file.write("task,vars,clauses\n")
        for name, data in task.items():
            out_file.write(f"{name},{data['vars']},{data['clauses']}\n")

if __name__ == "__main__":
    download()