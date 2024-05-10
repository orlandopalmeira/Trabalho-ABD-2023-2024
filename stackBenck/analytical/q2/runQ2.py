import os

RM_ALL_INDEXES=True

def execute_query(query: str):
    os.system(f"psql -c \"{query}\"")

# Índices da query 2, o segundo índice não é utilizado
indexes1 = """
CREATE INDEX idx_votes_creationdate ON votes (creationdate);
CREATE INDEX idx_users_creationdate ON users (creationdate);
"""
# Remoção do índice idx_users_creationdate que não é utilizado
rm_indexes_1 = """
DROP INDEX idx_users_creationdate;
"""
# Índice do users.creationdate que consegue ser utilizado
indexes2 = """
CREATE INDEX idx_users_creationdate_year on users (extract(year from creationdate));
"""
# Remoção de todos os índices
rm_all_indexes = """
DROP INDEX idx_votes_creationdate;
DROP INDEX idx_users_creationdate_year;
"""

def q2_sem_nada():
    print("Running query 2 (sem nada)...")
    os.system("./exec_query.sh q2/q2.sql -e > explain_q2_sem_nada.txt")
    os.system("python3 ./arg_script.py q2/q2.sql")
    print("Done!")
    
def q2_indexes_1():
    print("Running query 2 (indexes 1)...")
    execute_query(indexes1)
    os.system("./exec_query.sh q2/q2.sql -e > explain_q2_indexes_1.txt")
    os.system("python3 ./arg_script.py q2/q2.sql")
    execute_query(rm_indexes_1)
    print("Done!")

def q2_indexes_2():
    print("Running query 2 (indexes 2)...")
    execute_query(indexes2)
    os.system("./exec_query.sh q2/q2.sql -e > explain_q2_indexes_2.txt")
    os.system("python3 ./arg_script.py q2/q2.sql")
    print("Done!")

def q2_reestruturada():
    print("Running query 2 (reestruturada)...")
    os.system("./exec_query.sh q2/q2-final.sql -e > explain_q2-final.txt")
    os.system("python3 ./arg_script.py q2/q2-final.sql")
    print("Done!")

if __name__ == "__main__":
    q2_sem_nada() 
    q2_indexes_1()
    q2_indexes_2()
    q2_reestruturada() # usa todos os índices

    print("All done!")
    if RM_ALL_INDEXES:
        execute_query(rm_all_indexes)