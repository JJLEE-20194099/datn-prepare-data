
tmux new-session -d -s crawl 'python crawl_service.py' & \
tmux new-session -d -s clean 'python clean_service.py' & \
tmux new-session -d -s insert 'python insert_service.py' & \
uvicorn main:app --port 8885 --host 0.0.0.0 --reload
