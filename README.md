# CloudBrain websocket server module.

## Setup
```
pip install . --user
```
Or if you plan to modify the code:
```
pip install -e . --user
```

## Run
```
python -m cbws.run --file /path/to/conf.json
```

See [default config file](https://github.com/cloudbrain/cloudbrain-websocket-server/blob/master/examples/ws_server_config.json)

## Docker Compose
Ensure you have the cloudbrain_network created:
```
sudo docker network create cloudbrain_network
```
Then, run:
```
sudo docker-compose up
```

## Plain Docker
```
docker build -t cbws .
docker run -it -e PORT=31415 -e AUTH_URL=localhost -e RABBITMQ_ADDRESS=localhost -e RABBITMQ_USER=guest -e RABBITMQ_PWD=guest cbws
```
