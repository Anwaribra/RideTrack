import subprocess
import threading
import signal
import sys
import os

running = True

def run_script(script_name):
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    process = subprocess.Popen(
        [sys.executable, script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    while running and process.poll() is None:
        output = process.stdout.readline()
        if output:
            print(f"[{script_name}] {output.strip()}")
    if process.poll() is None:
        process.terminate()

def signal_handler(signum, frame):
    global running
    running = False

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    scripts = [
        "nyc_taxi_ingest.py",
        "gps_data_producer.py",
        "weather_api_ingest.py"
    ]
    
    threads = []
    for script in scripts:
        thread = threading.Thread(target=run_script, args=(script,))
        thread.start()
        threads.append(thread)
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()