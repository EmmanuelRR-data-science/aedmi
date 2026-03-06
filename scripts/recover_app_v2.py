import os, datetime

history_dirs = [
    r"C:\Users\EmmanuelRamírez\AppData\Roaming\Code\User\History",
    r"C:\Users\EmmanuelRamírez\AppData\Roaming\Cursor\User\History"
]

found = []
for h_dir in history_dirs:
    if not os.path.exists(h_dir): continue
    for root, dirs, files in os.walk(h_dir):
        for f in files:
            path = os.path.join(root, f)
            try:
                mtime = os.path.getmtime(path)
                with open(path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    # Look for something that unambiguously identifies app.py AND proves it's not truncated
                    if 'def api_operaciones_aeroportuarias():' in content and 'app = Flask(__name__)' in content:
                        found.append((mtime, path, content))
            except Exception:
                pass

found.sort(reverse=True)
if found:
    for mtime, path, c in found[:5]:
        print(f"FOUND: {path} with len {len(c)} bytes, modified {datetime.datetime.fromtimestamp(mtime)}")
        # Dump the best one
        if len(c) > 60000:  # Valid app.py is around 63KB
            print(f"Restoring from {path}")
            # wait, we must remove the poblacion route if it has it
            start_str = '@app.route("/api/ciudades/<slug>/poblacion-ocupada-turismo")'
            end_str = '        return jsonify({"error": str(e)}), 500\n'
            
            start_idx = c.find(start_str)
            if start_idx != -1:
                end_idx = c.find(end_str, start_idx)
                if end_idx != -1:
                    end_idx += len(end_str)
                    cleaned = c[:start_idx] + c[end_idx:]
                    with open('app.py', 'w', encoding='utf-8') as f:
                        f.write(cleaned)
                    print("Successfully recovered and stripped app.py")
                    exit(0)
            
            # If it didn't have the poblacion block, just write it entirely
            with open('app.py', 'w', encoding='utf-8') as f:
                f.write(c)
            print("Recovered app.py verbatim (no poblacion block to strip)")
            exit(0)
else:
    print("No backup found containing both functions!")

