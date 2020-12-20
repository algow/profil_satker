import os
from flask import Blueprint, jsonify, request
from werkzeug.utils import secure_filename
import pandas as pd
from bson.json_util import dumps
from configs.mongodb import mongo
from tasks.pagurealisasi import store_pagu
from application.globals import KANWIL

pagurealisasi_blueprint = Blueprint('pagurealisasi_blueprint', __name__)


@pagurealisasi_blueprint.route('/pagurealisasi', methods=['GET'])
def index():
  pagurealisasi = []

  try:
    pagurealisasi = list(mongo.db.pagurealisasi.find({'kode_kanwil': request.args.get('kanwil'), 'kode_satker': request.args.get('kode_satker')}))
  except:
    print('ERR')

  return dumps(pagurealisasi)


@pagurealisasi_blueprint.route('/pagurealisasi', methods=['POST'])
def store():
  message = {
    'status': 'sucess',
    'file': ''
  }

  file = request.files['file_excel']
  filename = os.path.join('publics/', secure_filename(file.filename))

  # try:
  file.save(filename)
  store_pagu(filename)
  # except:
  #   message['status'] = 'failed'

  message['file'] = filename

  return jsonify(message)