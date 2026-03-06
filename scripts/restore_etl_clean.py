import subprocess

img = 'bf597891ce6a'  # edmi-app-vps-cron from 18:29

print('Extracting pristine etl/run.py from cron image...')
result = subprocess.check_output(f'docker run --rm {img} cat /app/etl/run.py', shell=True).decode('utf-8', errors='ignore')

# Fix the known literal \n syntax error
result = result.replace('\\n    except Exception as e:', '    except Exception as e:')

with open('etl/run.py', 'w', encoding='utf-8') as f:
    f.write(result)

print(f'Restored etl/run.py ({len(result)} bytes) and fixed literal \\n')

# Verify it compiles
import py_compile
try:
    py_compile.compile('etl/run.py', doraise=True)
    print('etl/run.py compiles successfully!')
except py_compile.PyCompileError as e:
    print(f'Compile error: {e}')
