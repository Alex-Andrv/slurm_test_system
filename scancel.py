import subprocess

base = 907200
for i in range(10000):
    job_id = base + i
    command = ["scancel", str(job_id)]
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    if result.returncode == 0:
        print(f"Задача {job_id} была отменена")
    else:
        print(f"Задача {job_id} не была отменена. Какая-то проблемма")
