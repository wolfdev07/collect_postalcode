from flask import Flask, request, render_template, send_file
import os
from collect import collect_data  # Importa collect_data desde collect.py

app = Flask(__name__)

# Asegúrate de que el directorio de carga exista
STATIC_ROOT = 'static/'

zelda = os.path.join(app.config['STATIC_FILES'], 'data', 'collection.xls')

if not os.path.exists(STATIC_ROOT):
    os.makedirs(STATIC_ROOT)

app.config['STATIC_FILES'] = STATIC_ROOT

# INDEX
@app.route('/')
def index():
    return render_template('index.html')

# SUBIR ARCHIVO
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    # Verificar el método de solicitud
    if request.method == 'GET':
        return render_template('upload.html')

    if request.method == 'POST':

        if 'file' not in request.files:
            return 'No file part'
        
        file = request.files['file']
        if file.filename == '':
            return 'No selected file'
        if file:
            filename = os.path.join(app.config['STATIC_FILES'], 'data', file.filename)
            file.save(filename)
            # Llama a collect_data con el nombre del archivo
            return 'File successfully uploaded'

# DESCARGAR ARCHIVO ACTUAL
@app.route('/download')
def download_file():
    file_path = os.path.join(app.config['STATIC_FILES'], 'data', 'collection.xls')
    return send_file(file_path, as_attachment=True)


# ACTUALIZAR BASE DE DATOS
@app.route('/update_database')
def update_database():
    # Llama a collect_data con el nombre del archivo
    
    collect_data(zelda)
    return 'Database updated successfully'


if __name__ == "__main__":
    app.run(debug=True, port=5050)
