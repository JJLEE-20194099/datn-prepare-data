kill -9 $(lsof -t -i:5000)
fuser -k 5000/tcp
mlflow server --backend-store-uri postgresql://postgres:postgres@localhost:5432/datn --default-artifact-root file:/home/long/mlruns -h 0.0.0.0 -p 5000 --gunicorn-opts "--timeout 180"