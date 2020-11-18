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
  print(request.args.get('kode_satker'))
  try:
    pagurealisasi = list(mongo.db.pagurealisasi.find({'kode_kanwil': 25, 'kode_satker': int(request.args.get('kode_satker'))}))
    # pagurealisasi = mongo.db.pagurealisasi.aggregate([
    #   { 
    #     '$match': KANWIL
    #   },
    #   { 
    #     '$group': { 
    #       '_id': '$kode_satker',
    #       'dipa': { '$sum': '$dipa' },
    #       'blokir': { '$sum': '$blokir' },
    #       'outs_kontrak': { '$sum': '$outs_kontrak' },
    #       'realisasi': { '$sum': '$realisasi' },
    #     }
    #   }
    # ])
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

  try:
    file.save(filename)
    store_pagu(filename)
  except:
    message['status'] = 'failed'

  message['file'] = filename

  return jsonify(message)