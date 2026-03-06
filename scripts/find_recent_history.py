import os
import datetime

history_dirs = [
    r"C:\Users\EmmanuelRamírez\AppData\Roaming\Cursor\User\History",
    r"C:\Users\EmmanuelRamírez\AppData\Roaming\Code\User\History",
]

found_files = []
for h_dir in history_dirs:
    if os.path.exists(h_dir):
        for root, dirs, files in os.walk(h_dir):
            for f in files:
                filepath = os.path.join(root, f)
                try:
                    mtime = os.path.getmtime(filepath)
                    # Solamente recuperar si es muy reciente (últimas semanas)
                    if mtime < (datetime.datetime.now() - datetime.timedelta(days=15)).timestamp():
                        continue
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as file:
                        content = file.read()
                        if 'get_demografia_estatal' in content or '_normalizar_estado' in content or 'get_itaee_estatal' in content:
                            found_files.append((mtime, filepath))
                except Exception:
                    pass

found_files.sort(reverse=True)
for mtime, path in found_files[:10]:
    print(f"{datetime.datetime.fromtimestamp(mtime)}: {path}")
