cd /home/long/long/datn-feast
sh stop_feast.sh
tmux new-session -d -s feast_1 'sh start_feast.sh'
sh start_server.sh

