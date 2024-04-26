import subprocess, psutil, psycopg2, time

BASE_FILE = ""
FINAL_FILE = "-vfinal"
DBNAME = "stack"
USER = "postgres"
PASSWORD = "postgres"

def run_cmd(command: str):
    command_split = command.split(" ")
    subprocess.run(command_split)

def run_base(q_num: int):
    cmd = None
    file_path = f"q{q_num}/q{q_num}{BASE_FILE}.sql"
    read_file = open(file_path, "r")
    print(read_file.read())
    cmd = f"./avg_time.sh {file_path}"
    run_cmd(cmd)

def run_final(q_num: int):
    cmd = None
    file_path = f"q{q_num}/q{q_num}{FINAL_FILE}.sql"
    cmd = f"./avg_time.sh {file_path}"
    run_cmd(cmd)


def measure_resource_usage():
    cpu = psutil.cpu_percent(interval=1)
    # print("CPU Usage:", cpu_percent, "%")

    mem = psutil.virtual_memory().used
    # mem = psutil.virtual_memory()
    # print("Memory Usage:", mem.percent, "%")

    # disk = psutil.disk_usage('/')
    disk = psutil.disk_io_counters()
    # print("Disk Usage:", disk.percent, "%")

    return cpu, mem, disk


def get_query_base(q_num) -> str:
    file_path = f"q{q_num}/q{q_num}{BASE_FILE}.sql"
    with open(file_path, "r") as file:
        return file.read()

def get_query_final(q_num) -> str:
    file_path = f"q{q_num}/q{q_num}{FINAL_FILE}.sql"
    with open(file_path, "r") as file:
        return file.read()

def execute_query(query):
    conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD}")
    cur = conn.cursor()
    cur.execute(query)
    conn.close()

def measure_query_execution(query, times=3):
    cpu_before, mem_before, disk_before = measure_resource_usage()

    for _ in range(times):
        execute_query(query)

    cpu_after, mem_after, disk_after = measure_resource_usage()

    cpu_usage = cpu_after - cpu_before
    mem_usage = mem_after - mem_before
    # disk_usage = disk_after - disk_before

    # return cpu_usage, mem_usage, disk_usage
    return cpu_usage, mem_usage, disk_after


if __name__ == "__main__":
    q_num = 2
    query = get_query_base(q_num)
    cpu_usage, mem_usage, disk_usage = measure_query_execution(query, 1)

    print("CPU Usage:", cpu_usage, "%")
    print("Memory Usage:", mem_usage / (1024*1024), "MB")
    print("Disk Usage:", disk_usage)

    # run_final(1)
