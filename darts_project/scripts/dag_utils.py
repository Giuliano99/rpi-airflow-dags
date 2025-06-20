import os
import subprocess

def run_script(script_relative_path):
    def _run():
        script_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', *script_relative_path.split('/'))
        result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
        print(result.stdout)
    return _run
