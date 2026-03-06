import subprocess
import os
import re

print('Searching in Docker images...')
try:
    images_out = subprocess.check_output('docker images -a --format "{{.ID}}"', shell=True).decode()
    images = images_out.strip().split()
except Exception as e:
    print(f"Error getting images: {e}")
    exit(1)

found = False
for img in images[:30]:
    try:
        # Run a temporary container from the image to cat app.py
        result = subprocess.check_output(f'docker run --rm {img} cat /app/app.py', shell=True, stderr=subprocess.DEVNULL).decode('utf-8', errors='ignore')
        
        # Check if it has api_operaciones_aeroportuarias
        if 'def api_operaciones_aeroportuarias():' in result:
            if 'api_ciudad_poblacion_ocupada_turismo' not in result and 'app.run(host="0.0.0.0"' in result:
                print(f'Found pristine app.py in image {img}')
                with open('app.py', 'w', encoding='utf-8') as f:
                    f.write(result)
                print('Restored successfully.')
                found = True
                break
            elif 'api_ciudad_poblacion_ocupada_turismo' in result and 'app.run(' in result:
                # It has the pob block, and hasn't been truncated yet!
                print(f'Found pre-truncation app.py in image {img}')
                c = result
                start_str = '@app.route("/api/ciudades/<slug>/poblacion-ocupada-turismo")'
                end_str = '        return jsonify({"error": str(e)}), 500\n'
                
                # Careful stripping
                start_idx = c.find(start_str)
                if start_idx != -1:
                    end_idx = c.find(end_str, start_idx)
                    if end_idx != -1:
                        end_idx += len(end_str)
                        c = c[:start_idx] + c[end_idx:]
                
                with open('app.py', 'w', encoding='utf-8') as f:
                    f.write(c)
                print('Restored and stripped successfully.')
                found = True
                break
    except Exception:
        pass

if not found:
    print('Could not find a valid app.py in the recent docker images.')
