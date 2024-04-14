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
