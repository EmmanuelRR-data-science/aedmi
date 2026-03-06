import subprocess
import os

print('Searching in Docker images...')
try:
    images_out = subprocess.check_output('docker images -a --format "{{.ID}}"', shell=True).decode()
    images = images_out.strip().split()
except Exception as e:
    print(f'Error getting images: {e}')
    exit(1)

found = False
for img in images[:30]:
    try:
        # Run a temporary container from the image to cat data_sources.py
        result = subprocess.check_output(f'docker run --rm {img} cat /app/services/data_sources.py', shell=True, stderr=subprocess.DEVNULL).decode('utf-8', errors='ignore')
        
        # Check if it has the missing functions
        if 'def get_pib_sector_economico' in result and 'ESTADO_NOMBRE_TO_CODIGO' in result:
            print(f'Found pristine data_sources.py in image {img}!')
            
            # Write it
            with open('services/data_sources.py', 'w', encoding='utf-8') as f:
                f.write(result)
            print('Restored data_sources.py completely.')
            found = True
            break
    except Exception:
        pass

if not found:
    print('Could not find data_sources.py with the missing functions.')
