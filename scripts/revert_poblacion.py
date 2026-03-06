import re

# 1. app.py
with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()
pattern = r'@app\.route\(\"/api/ciudades/<slug>/poblacion-ocupada-turismo\"\).*?return jsonify\(\{\"error\": str\(e\)\}\), 500\n*'
text = re.sub(pattern, '', text, flags=re.DOTALL)
with open('app.py', 'w', encoding='utf-8') as f:
    f.write(text)

# 2. services/db.py
with open('services/db.py', 'r', encoding='utf-8') as f:
    text = f.read()
pattern1 = r'def save_poblacion_ocupada_turismo_bulk.*?return 0\n*'
pattern2 = r'def get_poblacion_ocupada_turismo_merida.*?return \[\]\n*'
text = re.sub(pattern1, '', text, flags=re.DOTALL)
text = re.sub(pattern2, '', text, flags=re.DOTALL)
with open('services/db.py', 'w', encoding='utf-8') as f:
    f.write(text)

# 3. templates/dashboard.html
with open('templates/dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# remove dropdown option
text = text.replace('<option value="poblacion-ocupada-turismo">Población Ocupada en Restaurantes y Hoteles</option>', "")

# remove JS
js_pattern = r'\} else if \(ind === \'poblacion-ocupada-turismo\'\).*?function fillCiudadTableCrecimiento'
# Wait we can just restore the dashboard.html from git since the last commit on dashboard.html was NOT today?
# BUT WAIT, the user modified dashboard.html in the last 11 days?
print("Done cleanup app and db")
