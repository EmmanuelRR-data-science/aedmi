import re

with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()

routes = re.findall(r'@app\.route\("([^"]+)"', text)
for r in routes:
    if 'estatal' in r or 'indicadores' in r or 'nacional' in r:
        print(r)
