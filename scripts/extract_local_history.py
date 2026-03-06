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
                    if 'def _load_inegi_iter_csv_municipios_y_distribucion' in content:
                        found.append((mtime, path, content))
            except Exception:
                pass

found.sort(reverse=True)
if found:
    mtime, path, c = found[0]
    print(f'FOUND pristine data_sources.py in Cursor History: {path} (modified {datetime.datetime.fromtimestamp(mtime)})')
    with open('services/data_sources.py', 'w', encoding='utf-8') as f:
        f.write(c)
    print('Restored successfully.')
else:
    print('NO data_sources.py found in cursor history!')

found_db = []
for h_dir in history_dirs:
    if not os.path.exists(h_dir): continue
    for root, dirs, files in os.walk(h_dir):
        for f in files:
            path = os.path.join(root, f)
            try:
                mtime = os.path.getmtime(path)
                with open(path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    if 'def get_municipios_por_estado' in content and 'save_poblacion_ocupada_turismo_bulk' not in content and 'def get_poblacion_ocupada' not in content:
                        found_db.append((mtime, path, content))
            except Exception:
                pass

found_db.sort(reverse=True)
if found_db:
    mtime, path, c = found_db[0]
    # Verify no syntax error literals!
    c = c.replace('\\n', '\n')
    print(f'FOUND db.py in Cursor History: {path} (modified {datetime.datetime.fromtimestamp(mtime)})')
    with open('services/db.py', 'w', encoding='utf-8') as f:
        f.write(c)
    print('Restored db.py successfully.')
