import subprocess

print('Searching in Docker images for data_sources.py...')
try:
    images_out = subprocess.check_output('docker images -a --format "{{.ID}}"', shell=True).decode()
    images = images_out.strip().split()
except Exception as e:
    print(f"Error fetching images: {e}")
    exit(1)

best_size = 0
best_content = ''
best_img = ''

for img in images[:30]:
    try:
        # Run a temporary container from the image to cat data_sources.py
        result = subprocess.check_output(f'docker run --rm {img} cat /app/services/data_sources.py', shell=True, stderr=subprocess.DEVNULL).decode('utf-8', errors='ignore')
        if len(result) > best_size:
            best_size = len(result)
            best_content = result
            best_img = img
    except Exception:
        pass

if best_size > 0:
    print(f'Found largest data_sources.py ({best_size} bytes) in image {best_img}. Restoring...')
    with open('services/data_sources.py', 'w', encoding='utf-8') as f:
        f.write(best_content)
    print('Restored successfully.')
else:
    print('Failed to find data_sources.py in any image.')
