from flask import Flask, render_template, request, send_from_directory, redirect, url_for
import os
from generatorV2 import run_generator

app = Flask(__name__)
OUTPUT_DIR = "downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        country = request.form['country'].strip().upper()
        filename = f"{country}_contacts.csv"
        output_path = os.path.join(OUTPUT_DIR, filename)

        run_generator(country, output_path)

        return redirect(url_for('download_page', filename=filename))
    return render_template('index.html')

@app.route('/download/<filename>')
def download_page(filename):
    return render_template('download.html', filename=filename)

@app.route('/files/<filename>')
def download_file(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
