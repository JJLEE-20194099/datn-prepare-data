cd /mnt/long/airflow/dags
kill -9 $(lsof -t -i:8080)
kill -9 $(lsof -t -i:8081)
tmux kill-session -t airflowscheduler
tmux new-session -d -s airflowscheduler 'airflow scheduler'
airflow webserver


