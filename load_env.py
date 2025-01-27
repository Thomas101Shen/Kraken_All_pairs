import os

def load_env(file_path=".env"):
    with open(file_path) as f:
        for line in f:
            key, value = line.strip().split('=', 1)
            os.environ[key] = value