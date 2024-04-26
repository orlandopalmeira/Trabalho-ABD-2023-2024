import subprocess, psutil, psycopg2, time

BASE_FILE = ""
FINAL_FILE = "-vfinal"
DBNAME = "stack"
USER = "postgres"
PASSWORD = "postgres"

TIMES = 3 # Number of times to execute the query for average

# Shell
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
###



def get_query_base(q_num) -> str:
    file_path = f"q{q_num}/q{q_num}{BASE_FILE}.sql"
    with open(file_path, "r") as file:
        return file.read()

def get_query_final(q_num) -> str:
    file_path = f"q{q_num}/q{q_num}{FINAL_FILE}.sql"
    with open(file_path, "r") as file:
        return file.read()

def execute_query(query: str):
    conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD}")
    cur = conn.cursor()
    cur.execute(query)
    res = cur.fetchall()
    conn.close()
    return res

def measure_query(query: str, times=TIMES):
    query_explain = f"EXPLAIN ANALYZE {query}"
    exec_times = []
    execute_query(query_explain) # Warm-up query
    for i in range(times):
        plan = execute_query(query_explain)
        # print(plan)
        time = plan[-1][0].split(" ")[-2] # in ms
        time = float(time) * 0.001 # converto to secs
        # print(f"Execution time: {time} secs")
        exec_times.append(time)
    avg = sum(exec_times) / len(exec_times)
    avg = round(avg, 3)
    return avg


#* Q1

#! WIP
def q1_change_arguments(query_str: str, interval: str) -> str:
    # query_str = query_str.replace("INTERVAL", f"'{interval}'") #! WIP
    return query_str

def q1_base(interval: str) -> float:
    query = get_query_base(1)
    # Change arguments
    query = q1_change_arguments(query, interval)
    avg = measure_query(query)
    return avg

def q1_final(interval: str) -> float:
    query = get_query_final(1)
    # Change arguments
    query = q1_change_arguments(query, interval)
    avg = measure_query(query)
    return avg
    


if __name__ == "__main__":

    arg = "6 months"
    avg = q1_base(arg)
    print(f"Average for q1-base with : {avg} secs")

    avg = q1_final("6 months")
    print(f"Average for q1-final: {avg} secs")
