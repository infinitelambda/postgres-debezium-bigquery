# Instructions on how to deploy Postgres-Debezium-BigQuery CDC solution


## The database
Deploy the PostgreSQL service to create the source database for debezium with the example data
```
kubectl apply -f postgres/
```

## The debezium server deployment
Next step is to build the local debezium-server image with the config and google service account.

So create a Google service account and copy paste the key.json into the debezium-fv-img/conf/gcp-auth.json file

Set the proper values in the conf/application.properties file regarding database name, schema list ...

Then run the following
```
cd debezium-fv-img
docker build -t debezium-server .
```

And the last step here is to deploy it to the cluster
```
kubectl apply -f debezium-server
```

