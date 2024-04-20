# Passo a passo para fazer o setup inicial local do cluster Spark

- Deploy the cluster (comando em baixo).
- Passar os csv para uma pasta app/stack/
- Correr a script `docker exec spark-spark-1 python3 gen_files.py` para criar os ficheiros parquet (pode demorar um pouco).
- De seguida, é correr a script `docker exec spark-spark-1 python3 main.py` para correr o job principal como normalmente.

# Comandos úteis

## Deploy the cluster:
docker compose -p spark up -d

## Deploy the cluster with Docker Compose:
docker compose -p spark up -d --scale spark-worker=3

## Start the history server (available at http://localhost:28080):
docker exec spark-spark-1 start-history-server.sh

## Execute the main.py job:
docker exec spark-spark-1 python3 main.py

## To stop the cluster:
docker compose -p spark stop

## To delete the cluster:
docker compose -p spark down

## Notas:
- É preciso dar permissão à diretoria em que ficheiros são escritos pelo spark, como spark.write.parquet()
chmod -R 777 app/
