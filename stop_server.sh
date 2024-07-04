tmux kill-session -t crawl
tmux kill-session -t clean
tmux kill-session -t insert
tmux kill-session -t crawlbot
kill $(lsof -t -i:8885)
