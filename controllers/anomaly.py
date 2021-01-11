from flask import Blueprint, jsonify, request
from bson.json_util import dumps
from configs.mongodb import mongo

anomalies_blueprint = Blueprint('anonamlies_blueprint', __name__)


@anomalies_blueprint.route('/anomali_persatker', methods=['GET'])
def anomali_persatker():
  data = {
    'paguminus': [],
    'realisasi_minus': [],
    'suspense': []
  }

  try:
    paguminus = mongo.db.paguminus.find({
      'kode_kanwil': request.args.get('kanwil'),
      'kode_satker': request.args.get('satker')
    })

    realisasi_minus = mongo.db.realisasi_minus.find({
      'kode_kanwil': request.args.get('kanwil'),
      'kode_satker': request.args.get('satker')
    })

    suspense = mongo.db.suspense.find({
      'kode_kanwil': request.args.get('kanwil'),
      'kode_satker': request.args.get('satker')
    })

    data['paguminus'] = paguminus
    data['realisasi_minus'] = realisasi_minus
    data['suspense'] = suspense
  except:
    print('ERR')

  return dumps(data)