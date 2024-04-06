## Deploy the cluster:
$ docker compose -p spark up -d

## Execute the main.py job:
$ docker exec spark-spark-1 python3 main.py

## To stop the cluster:
$ docker compose -p spark stop

## To delete the cluster:
$ docker compose -p spark down
