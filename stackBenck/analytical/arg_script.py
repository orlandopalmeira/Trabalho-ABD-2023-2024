import subprocess, psycopg2, time, sys
from prettytable import PrettyTable

DBNAME = "stack"
USER = "postgres"
PASSWORD = "postgres"

TIMES = 3 # Number of times to execute the query for average

# Shell
def run_cmd(command: str):
    command_split = command.split(" ")
    subprocess.run(command_split)


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
        print(f"Execution time: {time} secs")
        exec_times.append(time)
    avg = sum(exec_times) / len(exec_times)
    avg = round(avg, 3)
    return avg


#* Q1
def q1_change_arguments(query_str: str, interval: str) -> str:
    query_str = query_str.replace("6 months", f"{interval}")
    return query_str

def create_q1_table(query_str):
    myTable = PrettyTable(["Arguments", "Time (s)"])
    args = ["1 month", "3 months", "6 months", "1 year", "2 years"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        query = q1_change_arguments(query_str, arg)
        avg_final = measure_query(query)
        myTable.add_row([arg, avg_final])
    print(myTable)
    return myTable

#* Q2
def q2_change_arguments(query_str: str, interval: str, bucketInterval: str) -> str:
    query_str = query_str.replace("5000", f"{bucketInterval}")
    query_str = query_str.replace("5 year", f"{interval}")
    return query_str

def create_q2_table(query_str):
    myTable = PrettyTable(["Interval", "Bucket", "Time (s)"])
    args_interval = ["1 year", "2 years", "3 years", "4 years", "5 years"] # Provisorio
    args_bucket = ["5000", "10000", "15000", "20000", "25000"] # Provisorio
    for arg_i in args_interval:
        for arg_b in args_bucket:
            print(f"Running query with arguments: {arg_i}, {arg_b}")
            query = q2_change_arguments(query_str, arg_i, arg_b)
            avg_final = measure_query(query)
            myTable.add_row([arg_i, arg_b, avg_final])
    print(myTable)
    return myTable

#* Q3 - #!WIP
def q3_change_arguments(query_str: str, minim: str) -> str:
    query_str = query_str.replace("10", f"{minim}")
    return query_str

def create_q3_table(query_str):
    myTable = PrettyTable(["Arguments", "Time (s)"])
    args = ["10", "20", "30", "40", "50"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        query = q3_change_arguments(query_str, arg)
        avg_final = measure_query(query)
        myTable.add_row([arg, avg_final])
    print(myTable)
    return myTable

#* Q4 - #!WIP
def q4_change_arguments(query_str: str, bucketSize: str) -> str:
    query_str = query_str.replace("1 minute", f"{bucketSize}")
    return query_str

def create_q4_table(query_str):
    myTable = PrettyTable(["Arguments", "Time (s)"])
    args = ["1 minute", "10 minutes", "30 minutes", "1 hour", "6 hours"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        query = q4_change_arguments(query_str, arg)
        avg_final = measure_query(query)
        myTable.add_row([arg, avg_final])
    print(myTable)
    return myTable

if __name__ == "__main__":
    # python3 arg_script.py file.sql
    if len(sys.argv) < 2:
        print("Usage: python3 arg_script.py file.sql")
        sys.exit(1)
    file_sql = sys.argv[1]
    with open(file_sql, "r") as f:
        query_str = f.read()

    if "q1" in file_sql:
        create_q1_table(query_str)
    elif "q2" in file_sql:
        create_q2_table(query_str)
    elif "q3" in file_sql:
        create_q3_table(query_str)
    elif "q4" in file_sql:
        create_q4_table(query_str)