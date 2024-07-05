kill $(lsof -t -i:8885)
tmux kill-session -t crawl
tmux kill-session -t clean
tmux kill-session -t insert

