import os

history_dir = r"C:\Users\EmmanuelRamírez\AppData\Roaming\Code\User\History"
found_files = []
for root, dirs, files in os.walk(history_dir):
    for f in files:
        if f.endswith('.py') or '.' not in f:
            filepath = os.path.join(root, f)
            try:
                with open(filepath, 'r', encoding='utf-8') as file:
                    content = file.read()
                    if 'ESTADO_NOMBRE_TO_CODIGO' in content and 'def _fetch_inflacion_nacional_banxico' in content:
                        mtime = os.path.getmtime(filepath)
                        found_files.append((mtime, filepath))
            except Exception:
                pass

found_files.sort(reverse=True)
import datetime
for mtime, path in found_files[:10]:
    print(f"{datetime.datetime.fromtimestamp(mtime)}: {path}")
