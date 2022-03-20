import os
from flask import Flask, render_template, request



UPLOAD_FOLDER = '/uploads'
app = Flask(__name__, template_folder='./templates')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.debug = True


@app.route("/")
def index():
    return "hello world!"


@app.route("/import")
def import_dump():
    #import_db('./data/dump.csv')
    return "import dump"


@app.route("/create_dump")
def create_dump():
    #    export_table_db()
    return "create dump"


@app.route('/upload')
def upload():
    return render_template('upload.html')


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        f.save('./uploads/'+f.filename)
        return 'file uploaded successfully'
