from flask import Flask, render_template, request, send_file
import os
from generatorV2 import run_generator

app = Flask(__name__)
OUTPUT_DIR = "downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        country = request.form['country'].strip().upper()
        out_path = os.path.join(OUTPUT_DIR, f"{country}_contacts.csv")
        run_generator(country, out_path)
        return send_file(out_path, as_attachment=True)
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
