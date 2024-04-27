# Passo a passo para fazer o setup inicial local do cluster Spark

- Deploy the cluster (comando em baixo).
- Passar os csv para uma pasta app/stack/
- Correr a script `docker exec spark-spark-1 python3 gen_files.py` para criar os ficheiros parquet (pode demorar um pouco).
- De seguida, é correr a script `docker exec spark-spark-1 python3 main.py` para correr o job principal como normalmente. Para correr apenas um certo workload pode-se usar o comando `docker exec spark-spark-1 python3 main.py w1` por exemplo, ou até `./main_run.sh w1`.

# Comandos úteis

## Deploy the cluster:
docker compose -p spark up -d

## Deploy the cluster with Docker Compose:
docker compose -p spark up -d --scale spark-worker=3
<!-- ou -->
./raise_cluster.sh

## Start the history server (available at http://localhost:28080):
docker exec spark-spark-1 start-history-server.sh

## Execute the main.py job:
docker exec spark-spark-1 python3 main.py
docker exec spark-spark-1 python3 gen_files.py

## To stop the cluster:
docker compose -p spark stop

## To delete the cluster:
docker compose -p spark down

## Notas:
- É preciso dar permissão à diretoria em que ficheiros são escritos pelo spark, como spark.write.parquet()
chmod -R 777 app/
