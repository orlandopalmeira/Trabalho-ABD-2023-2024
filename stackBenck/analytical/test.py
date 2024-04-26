import time
import psutil
import psycopg2

def measure_resource_usage():
    cpu_before = psutil.cpu_percent(interval=1)

    mem_before = psutil.virtual_memory().used

    disk_before = psutil.disk_io_counters()

    return cpu_before, mem_before, disk_before

def execute_query(query):
    conn = psycopg2.connect("dbname=mydatabase user=myuser password=mypassword")
    cur = conn.cursor()
    cur.execute(query)
    conn.close()

def measure_query_execution(query):
    cpu_before, mem_before, disk_before = measure_resource_usage()

    execute_query(query)

    cpu_after, mem_after, disk_after = measure_resource_usage()

    cpu_usage = cpu_after - cpu_before
    mem_usage = mem_after - mem_before
    disk_usage = disk_after - disk_before

    return cpu_usage, mem_usage, disk_usage

if __name__ == "__main__":
    query = "SELECT * FROM your_table"  
    cpu_usage, mem_usage, disk_usage = measure_query_execution(query)

    print("CPU Usage:", cpu_usage, "%")
    print("Memory Usage:", mem_usage / (1024*1024), "MB")
    print("Disk Usage:", disk_usage)
