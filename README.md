address-poc/
├── docker-compose.yml
├── Dockerfile.pyspark
├── requirements.txt
├── data/
│   └── addresses.csv
├── sql/
│   └── init.sql
└── src/
    └── classify.py


## Build
docker-compose up -d --build

## Verify Java installed correctly (optional sanity check)
```
docker exec -it address_poc_spark java -version
```

### You should see an OpenJDK version print out (likely 17 or 21 depending on what trixie currently ships).Then run your job as before
```
docker exec -it address_poc_spark spark-submit /app/src/classify.py
```

