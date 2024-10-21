# nft-framework

# Starting the project
1. Provide secrets.txt file for AWS connection and OpenSea API connection

2. Add custom airflow.env file under /data_extraction directory

3. Start docker containers
```bash
docker-compose up -d
```
4. Go into vault container to run a script that will initalize vault service and add keys specified in a file
```bash
docker exec -it vault /bin/sh 
/vault/scripts/vault-setup.sh
```
* might be needed to add relevant permissions on file
```bash
chmod 644 /vault/scripts/secrets.txt
chmod +x /vault/scripts/vault-setup.sh
```

building (only airflow service requires some dependencies)
docker-compose -f docker-compose.airflow.yml build

starting
docker-compose -f docker-compose.airflow.yml up -d  

stoping
docker-compose -f docker-compose.airflow.yml down/stop


## todo's


