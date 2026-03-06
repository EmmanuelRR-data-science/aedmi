import os

endpoint_code = '''
@app.route("/api/ciudades/<slug>/poblacion-ocupada-turismo")
def api_ciudad_poblacion_ocupada_turismo(slug):
    """
    Retorna la poblacion ocupada en restaurantes y hoteles.
    Actualmente solo aplica a Mérida ("merida").
    """
    if slug != "merida":
        return jsonify([])
    try:
        from services.db import get_poblacion_ocupada_turismo_merida
        data = get_poblacion_ocupada_turismo_merida()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
'''

with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()

# find "if __name__"
pos = text.rfind("if __name__")
if pos != -1:
    new_text = text[:pos] + endpoint_code + "\\n\\n" + text[pos:]
    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(new_text)
    print("Endpoint inserted.")
else:
    print("Failed to find main block.")
