import subprocess, psycopg2, time, sys
from prettytable import PrettyTable

DBNAME = "stack"
USER = "postgres"
PASSWORD = "postgres"
HOST = "127.0.0.1"
PORT = "5432"

TIMES = 2 # Number of times to execute the query for average

# Shell
def run_cmd(command: str):
    command_split = command.split(" ")
    subprocess.run(command_split)


def execute_query(query: str):
    conn = psycopg2.connect(f"dbname={DBNAME} user={USER} password={PASSWORD} host={HOST} port={PORT}")
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
    STANDARD = "6 months"
    query_str_res = query_str.replace(STANDARD, f"{interval}")
    if STANDARD != interval and query_str_res == query_str:
        print(f"ERROR while switching q1 args. No arguments were changed in query. Exiting...")
        sys.exit(1)
    elif STANDARD == interval and query_str_res != query_str:
        print(f"WARNING: Standard argument changed query 1!!")
        sys.exit(1)
    return query_str_res

def create_q1_table(query_str):
    myTable = PrettyTable(["Arguments", "Time (s)"])
    args = ["1 month", "3 months", "6 months", "1 year", "2 year"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        query = q1_change_arguments(query_str, arg)
        avg_final = measure_query(query)
        myTable.add_row([arg, avg_final])
    print(myTable)
    return myTable

def compare_q1_results(query_1, query_2):
    # Compare the results of the queries
    args = ["3 months", "6 months", "1 year", "2 year"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        # Remove LIMIT 100 from queries
        new_query_1 = q1_change_arguments(query_1, arg)
        new_query_2 = q1_change_arguments(query_2, arg)
        new_query_1 = query_1.replace("LIMIT 100", "")
        new_query_2 = query_2.replace("LIMIT 100", "")
        res1 = execute_query(new_query_1)
        res2 = execute_query(new_query_2)
        res1 = sorted(res1)
        res2 = sorted(res2)
        if res1 == res2:
            print("Results are equal!")
        else:
            print("Results are different!")

#* Q2
def q2_change_arguments(query_str: str, interval: str, bucketInterval: str) -> str:
    STANDARD_BUCKET = "5000"
    STANDARD_INTERVAL = "5 year"
    query_str_res = query_str.replace(STANDARD_BUCKET, f"{bucketInterval}")
    query_str_res = query_str.replace(STANDARD_INTERVAL, f"{interval}")
    if STANDARD_INTERVAL != interval and STANDARD_BUCKET != bucketInterval and query_str_res == query_str:
        print(f"ERROR while switching q2 args. No arguments were changed in query. Exiting...")
        sys.exit(1)
    elif STANDARD_INTERVAL == interval and STANDARD_BUCKET == bucketInterval and query_str_res != query_str:
        print(f"WARNING: Standard argument changed query 2!!")
        sys.exit(1)
    return query_str_res

def create_q2_table(query_str):
    myTable = PrettyTable(["Interval", "Bucket", "Time (s)"])
    # args_interval = ["1 year", "2 year", "3 year", "4 year", "5 year"]
    # args_bucket = ["5000", "10000", "15000", "20000", "25000"]
    args_interval = ["1 year", "5 year", "7 year"]
    args_bucket = ["5000", "15000", "25000"]
    for arg_i in args_interval:
        for arg_b in args_bucket:
            print(f"Running query with arguments: {arg_i}, {arg_b}")
            query = q2_change_arguments(query_str, arg_i, arg_b)
            avg_final = measure_query(query)
            myTable.add_row([arg_i, arg_b, avg_final])
    print(myTable)
    return myTable

def compare_q2_results(query_1, query_2):
    # Compare the results of the queries
    # args_interval = ["1 year", "2 year", "3 year", "4 year", "5 year"]
    # args_bucket = ["5000", "10000", "15000", "20000", "25000"]
    args_interval = ["1 year", "5 year", "7 year"]
    args_bucket = ["5000", "15000", "25000"]
    for arg_i in args_interval:
        for arg_b in args_bucket:
            print(f"Running query with arguments: {arg_i}, {arg_b}")
            new_query_1 = q2_change_arguments(query_1, arg_i, arg_b)
            new_query_2 = q2_change_arguments(query_2, arg_i, arg_b)
            res1 = execute_query(new_query_1)
            res2 = execute_query(new_query_2)
            res1 = sorted(res1)
            res2 = sorted(res2)
            if res1 == res2:
                print("Results are equal!")
            else:
                print("Results are different!")
    

#* Q3
def q3_change_arguments(query_str: str, minim: str) -> str:
    STANDARD = "10"
    query_str_res = query_str.replace(STANDARD, f"{minim}")
    if STANDARD != minim and query_str_res == query_str:
        print(f"ERROR while switching q3 args. No arguments were changed in query. Exiting...")
        sys.exit(1)
    elif STANDARD == minim and query_str_res != query_str:
        print(f"WARNING: Standard argument changed query 3!!")
        sys.exit(1)
    return query_str_res

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

def compare_q3_results(query_1, query_2):
    # Compare the results of the queries
    args = ["10", "20", "30", "40", "50"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        new_query_1 = q3_change_arguments(query_1, arg)
        new_query_2 = q3_change_arguments(query_2, arg)
        res1 = execute_query(new_query_1)
        res2 = execute_query(new_query_2)
        res1 = sorted(res1)
        res2 = sorted(res2)
        if res1 == res2:
            print("Results are equal!")
        else:
            print("Results are different!")

#* Q4
def q4_change_arguments(query_str: str, bucketSize: str) -> str:
    STANDARD = "1 minute"
    query_str_res = query_str.replace(STANDARD, f"{bucketSize}")
    if STANDARD != bucketSize and query_str_res == query_str:
        print(f"ERROR while switching q4 args. No arguments were changed in query. Exiting...")
        sys.exit(1)
    elif STANDARD == bucketSize and query_str_res != query_str:
        print(f"WARNING: Standard argument changed query 4!!")
        sys.exit(1)
    return query_str_res

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

def compare_q4_results(query_1, query_2):
    # Compare the results of the queries
    args = ["1 minute", "10 minutes", "30 minutes", "1 hour", "6 hours"]
    for arg in args:
        print(f"Running query with argument: {arg}")
        new_query_1 = q4_change_arguments(query_1, arg)
        new_query_2 = q4_change_arguments(query_2, arg)
        res1 = execute_query(new_query_1)
        res2 = execute_query(new_query_2)
        res1 = sorted(res1)
        res2 = sorted(res2)
        if res1 == res2:
            print("Results are equal!")
        else:
            print("Results are different!")

if __name__ == "__main__":
    # python3 arg_script.py file.sql [file2.sql] --> (opcional para comparar scripts)
    if len(sys.argv) < 2:
        print("Usage: python3 arg_script.py file.sql [file2.sql]")
        sys.exit(1)

    file_sql = sys.argv[1]
    with open(file_sql, "r") as f:
        query_str = f.read()

    compare = False

    if len(sys.argv) == 3:
        compare = True
        print("Comparing scripts...")
        file_2 = sys.argv[2]
        with open(file_2, "r") as f:
            query_str_2 = f.read()
        
    if "q1" in file_sql:
        if compare:
            compare_q1_results(query_str, query_str_2)
        else:
            create_q1_table(query_str)
    elif "q2" in file_sql:
        if compare:
            compare_q2_results(query_str, query_str_2)
        else:
            create_q2_table(query_str)
    elif "q3" in file_sql:
        if compare:
            compare_q3_results(query_str, query_str_2)
        else:
            create_q3_table(query_str)
    elif "q4" in file_sql:
        if compare:
            compare_q4_results(query_str, query_str_2)
        else:
            create_q4_table(query_str)