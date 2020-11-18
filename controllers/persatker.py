from flask import Blueprint, jsonify, request
from bson.json_util import dumps
from configs.mongodb import mongo
from application.globals import KANWIL

persatker_blueprint = Blueprint('persatker_blueprint', __name__)


@persatker_blueprint.route('/refsatker', methods=['GET'])
def referensi():
  refsatker = []

  try:
    refsatker = list(mongo.db.ref_satker.find(KANWIL, { '_id': 0, 'kode_satker': 1, 'nama_satker': 1 }))
  except:
    print('ERROR')

  return dumps(refsatker)


@persatker_blueprint.route('/persatker', methods=['GET'])
def index():
  persatker = []
  
  try:
    persatker = list(mongo.db.per_satker.aggregate([
    {
      '$lookup': {
        'from': 'ref_satker',
        'localField': 'kode_satker',
        'foreignField': 'kode_satker',
        'as': 'referensi'
      } 
    }
  ]))
  except:
    print('error')

  return dumps(persatker)