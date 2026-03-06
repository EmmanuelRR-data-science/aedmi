import re

with open('etl/run.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Remove the syntax error `\n`
text = text.replace('\\n    except Exception as e:', '    except Exception as e:')

# 2. Revert the _scrape_poblacion_ocupada_observatur block
pattern1 = r'        # NUEVO INDICADOR MÉRIDA.*?save_poblacion_ocupada_turismo_bulk\(pob_data\)\n'
text = re.sub(pattern1, '', text, flags=re.DOTALL)

with open('etl/run.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Cleaned etl/run.py")
