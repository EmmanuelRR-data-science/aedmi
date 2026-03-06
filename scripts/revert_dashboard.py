import re

with open('templates/dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Remove the dropdown option
option_target = '''\\n                            <option value="poblacion-ocupada-turismo">Población Ocupada en Restaurantes y Hoteles</option>'''
text = text.replace(option_target, "")

# 2. Remove the IF block inside loadIndicadorCiudad
fetch_target = '''        } else if (ind === 'poblacion-ocupada-turismo') {
            fetch('/api/ciudades/' + encodeURIComponent(slug) + '/poblacion-ocupada-turismo')
                .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
                .then(function (result) {
                    if (!result.ok) {
                        msg.textContent = result.data.error || 'Error al cargar';
                        return;
                    }
                    var data = result.data || [];
                    placeholder.hidden = true;
                    wrapper.hidden = false;
                    tableSection.hidden = false;
                    if (actionsCiudad) actionsCiudad.hidden = false;
                    indicadorCiudadData = data;
                    indicadorCiudadTipo = 'poblacion-ocupada-turismo';
                    document.getElementById('indicador-chart-source-ciudad').textContent = 'Fuente: Observatorio Turístico de Yucatán - Encuesta Nacional de Empleo Trimestral (INEGI)';
                    if (resumenKpis) resumenKpis.hidden = true; 
                    renderPoblacionOcupadaTurismoCiudad(data, ciudadNombre);
                    fillCiudadTablePoblacionOcupadaTurismo(data);
                })
                .catch(function (err) { msg.textContent = 'Error: ' + err.message; });
        } else {
            msg.textContent = 'Seleccione un indicador.';
        }
    }

    function renderPoblacionOcupadaTurismoCiudad(data, ciudadNombre) {
        if (!data || typeof Plotly === 'undefined') return;
        var container = document.getElementById('indicador-chart-plot-ciudad');
        if (!container) return;
        var cfg = typeof getChartConfig === 'function' ? getChartConfig() : { palette: ['#0576F3', '#36F48C', '#F47806'], fontSize: 14, titleSize: 16, fontFamily: 'Aptos Light, sans-serif' };
        var palette = cfg.palette || ['#0576F3', '#36F48C', '#F47806'];
        
        var xLabels = data.map(function(d) { return d.anio + "-T" + d.trimestre; });
        var yValues = data.map(function(d) { return d.poblacion_ocupada; });
        
        var layout = {
            title: { text: '<b>Población Ocupada en Restaurantes y Hoteles - ' + ciudadNombre + '</b>', font: { family: cfg.fontFamily, size: cfg.titleSize }, x: 0.5, xanchor: 'center' },
            font: { family: cfg.fontFamily, size: cfg.fontSize },
            xaxis: { title: 'Trimestre', tickangle: -45 },
            yaxis: { title: 'Personas Ocupadas', tickformat: ',d', gridcolor: 'rgba(0,0,0,0.08)' },
            plot_bgcolor: 'white', paper_bgcolor: 'transparent',
            margin: { l: 70, r: 70, t: 90, b: 120 }, height: 460, showlegend: false,
            autosize: true
        };
        
        Plotly.newPlot(container, [
            { 
                type: 'scatter', 
                mode: 'lines+markers',
                x: xLabels, 
                y: yValues, 
                name: 'Población Ocupada', 
                line: { color: palette[0], width: 3 },
                marker: { color: palette[0], size: 8 }
            }
        ], layout, { responsive: true, displayModeBar: true, locale: 'es' });
    }

    function fillCiudadTablePoblacionOcupadaTurismo(data) {
        var tbody = document.getElementById('kpis-table-ciudad-body');
        var thead = document.getElementById('kpis-table-ciudad-thead');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (thead) thead.innerHTML = '<th>Año y Trimestre</th><th>Población Ocupada</th>';
        if (!data) return;
        data.forEach(function (r) { 
            var tr = document.createElement('tr'); 
            tr.innerHTML = '<td>' + String(r.anio) + ' T' + String(r.trimestre) + '</td><td>' + Number(r.poblacion_ocupada || 0).toLocaleString('es-MX') + '</td>'; 
            tbody.appendChild(tr); 
        });
    }'''

fetch_new = '''        } else {
            msg.textContent = 'Seleccione un indicador.';
        }
    }'''

text = text.replace(fetch_target, fetch_new)

with open('templates/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
print('Dashboard reverted successfully.')
