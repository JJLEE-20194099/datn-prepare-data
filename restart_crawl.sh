kill -9 $(lsof -t -i:8885)
uvicorn main:app --port 8885 --host 0.0.0.0 --reload