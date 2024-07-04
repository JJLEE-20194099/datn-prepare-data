cd /mnt/long/airflow/dags
kill $(lsof -t -i:8080)
kill $(lsof -t -i:8081)
tmux kill-session -t airflowscheduler
tmux new-session -d -s airflowscheduler 'airflow scheduler'
airflow webserver


