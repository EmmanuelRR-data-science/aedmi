import os

history_dir = r"C:\Users\EmmanuelRamírez\AppData\Roaming\Cursor\User\History"
found_files = []
if os.path.exists(history_dir):
    for root, dirs, files in os.walk(history_dir):
        for f in files:
            if f.endswith('.py') or '.' not in f:
                filepath = os.path.join(root, f)
                try:
                    with open(filepath, 'r', encoding='utf-8') as file:
                        content = file.read()
                        if 'ESTADO_NOMBRE_TO_CODIGO' in content:
                            mtime = os.path.getmtime(filepath)
                            found_files.append((mtime, filepath))
                except Exception:
                    pass

    found_files.sort(reverse=True)
    import datetime
    for mtime, path in found_files[:20]:
        print(f"{datetime.datetime.fromtimestamp(mtime)}: {path}")
else:
    print("Cursor history directory not found")
