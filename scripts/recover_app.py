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
                    if 'def api_operaciones_aeroportuarias():' in content and 'poblacion-ocupada-turismo' in content:
                        found.append((mtime, path, content))
            except Exception:
                pass

found.sort(reverse=True)
if found:
    best_mtime, best_path, best_content = found[0]
    print(f"Recovering from {best_path} (modified {datetime.datetime.fromtimestamp(best_mtime)})")
    import re
    # Remove ONLY the specific poblacion block without dotall
    # Instead of regex, we can just slice string
    start_str = '@app.route("/api/ciudades/<slug>/poblacion-ocupada-turismo")'
    end_str = '        return jsonify({"error": str(e)}), 500'
    
    start_idx = best_content.find(start_str)
    if start_idx != -1:
        end_idx = best_content.find(end_str, start_idx) + len(end_str)
        cleaned = best_content[:start_idx] + best_content[end_idx:]
        with open('app.py', 'w', encoding='utf-8') as f:
            f.write(cleaned)
        print("Successfully recovered and reverted app.py")
    else:
         with open('app.py', 'w', encoding='utf-8') as f:
            f.write(best_content)
            print("Wrote best content but couldn't find start_str")
else:
    print("No backup found containing both functions!")

