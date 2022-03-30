from http.client import HTTPException
from io import BytesIO

import dvc.api
import pandas as pd
from flask import Flask, render_template, request, send_file, json

from create_dump import export_table_db

UPLOAD_FOLDER = '/uploads'
app = Flask(__name__, template_folder='./templates')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.debug = True
columns = ["id", "id_cennika", "cena_netto", "dlugosc_loj", "id_model_apr", "data_od", "data_do", "id_taryfy",
           "id_kwota_zobow", "id_kwota_rachunku", "vat_id", "subsyd_apr", "kierunek_migracji", "id_grupa_apr",
           "metoda_platnosci", "operator"]


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/import")
def import_dump():
    # import_db('./data/dump.csv')
    return "import dump"


@app.route("/create_dump")
def create_dump():
    export_table_db('localhost', '5432', 'postgres', 'postgres', 'postgres', 'dvc_db', 'price', '\u007F',
                    'KP_CENA_MOD_APR.DAT')
    return "create dump"


@app.route('/upload')
def upload():
    return render_template('upload.html')


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        f.save('./uploads/' + f.filename)
        return 'file uploaded successfully'


@app.route('/version/<rev1>/diff/<rev2>/store/<storage>', methods=['GET'])
def delta(rev1: str, rev2: str, storage: str):
    with dvc.api.open('./data/KP_CENA_MOD_APR.DAT', "./", rev1, storage, mode='r', encoding='utf-8') as f1:
        data_frame1 = pd.DataFrame(pd.read_csv(f1, sep='\u007F', names=columns))

    with dvc.api.open('./data/KP_CENA_MOD_APR.DAT', "./", rev2, storage, mode='r', encoding='utf-8') as f2:
        data_frame2 = pd.DataFrame(pd.read_csv(f2, sep='\u007F', names=columns))

    data_frame3 = pd.concat([data_frame1, data_frame2]).drop_duplicates(keep=False)
    compression_opts = dict(method='zip', archive_name='apply_patch.DAT')
    data_frame3.to_csv('./data/apply_patch.zip', sep='\u007F', header=False, index=False, compression=compression_opts)

    path = "./data/apply_patch.zip"
    return send_file(path, as_attachment=True)


@app.route('/version/<repo>/<version>/store/<store>/dump/<dump>',
           defaults={'repo': './', 'dump': 'KP_CENA_MOD_APR.DAT'})
@app.route('/version/<version>/store/<store>', defaults={'repo': './', 'dump': 'KP_CENA_MOD_APR.DAT'})
def version(version: str, store: str, dump: str, repo):
    tmp_file = dump.split('.')[0]
    file_name: str = f"./data/{tmp_file}_{version}.DAT".format(tmp_file, version)
    with dvc.api.open('./data/' + dump, repo, version.upper(), store, mode='r', encoding='utf-8') as fd:
        data_frame = pd.DataFrame(pd.read_csv(fd, sep='\u007F', names=columns))
        compression_opts = dict(method='zip', archive_name=f"{tmp_file}_{version}.DAT".format(tmp_file, version))
        data_frame.to_csv('./data/download.zip', sep='\u007F', header=False, index=False, compression=compression_opts)

        with open('./data/download.zip', 'rb') as fh:
            buf = BytesIO(fh.read())

        return send_file(buf, as_attachment=True, mimetype="text/plain",
                         attachment_filename="download.zip".format(tmp_file, version))


@app.errorhandler(dvc.exceptions.FileMissingError)
def handle_bad_request(e):
    return 'File not found!', 404


@app.errorhandler(dvc.exceptions.PathMissingError)
def handle_bad_request(e):
    return 'File not found!', 404


@app.errorhandler(dvc.scm.base.RevError)
def handle_bad_request(e):
    return 'File not found!', 404


@app.errorhandler(HTTPException)
def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response
