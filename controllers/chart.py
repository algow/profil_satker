from flask import Blueprint, jsonify
from bson.json_util import dumps
from configs.mongodb import mongo
from application.globals import KANWIL
from utils import query_to_chart

chart_blueprint = Blueprint('chart_blueprint', __name__)


@chart_blueprint.route('/chart_kabupaten', methods=['GET'])
def chart_kabupaten():
  chart_data = []
  
  try:
    chart_data = list(mongo.db.per_kabupaten.aggregate([
    {
      '$sort': { 'realisasi': -1 }
    },
    {
      '$lookup': {
        'from': 'ref_kabupaten',
        'localField': 'kode_kabupaten',
        'foreignField': 'kode_kabupaten',
        'as': 'referensi'
      },
    }, {
      '$project' : { '_id': 0, 'nama_kabupaten': '$referensi.kabupaten', 'realisasi': 1 }
    }]))
  except:
    print('ERROR')

  return dumps(query_to_chart(chart_data))