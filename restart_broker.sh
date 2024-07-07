tmux kill-session -t broker_0
tmux kill-session -t broker_1
tmux kill-session -t broker_2
tmux new-session -d -s broker_0 'sudo /home/kafka/bin/kafka-server-start.sh /home/kafka/config/server_0.properties'
tmux new-session -d -s broker_1 'sudo /home/kafka/bin/kafka-server-start.sh /home/kafka/config/server_1.properties'
tmux new-session -d -s broker_2 'sudo /home/kafka/bin/kafka-server-start.sh /home/kafka/config/server_2.properties'