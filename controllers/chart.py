from flask import Blueprint, jsonify
from bson.json_util import dumps
from configs.mongodb import mongo
from application.globals import KANWIL
from utils import total_to_chart, query_to_chart

chart_blueprint = Blueprint('chart_blueprint', __name__)


@chart_blueprint.route('/chart_total', methods=['GET'])
def chart_total():
  chart_data = []
  
  try:
    chart_data = list(mongo.db.totals.find(KANWIL, { '_id': 0, 'pagu': 1, 'realisasi': 1 }))
  except:
    print('ERROR')

  return dumps(total_to_chart(chart_data))


@chart_blueprint.route('/chart_jenisbelanja', methods=['GET'])
def chart_jenisbelanja():
  chart_data = []
  
  try:
    chart_data = list(mongo.db.jenis_belanja.find(KANWIL, { '_id': 0, 'jenis': 1, 'realisasi': 1 }).sort('realisasi', -1))
  except:
    print('ERROR')

  return dumps(query_to_chart(chart_data))


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


@chart_blueprint.route('/chart_kppn', methods=['GET'])
def chart_kppn():
  chart_data = []
  
  try:
    chart_data = list(mongo.db.per_kppn.aggregate([
    {
      '$sort': { 'realisasi': -1 }
    },
    {
      '$lookup': {
        'from': 'ref_kppn',
        'localField': 'kode_kppn',
        'foreignField': 'kode_kppn',
        'as': 'referensi'
      },
    }, {
      '$project' : { '_id': 0, 'nama_kppn': '$referensi.kppn', 'realisasi': 1 }
    }]))
  except:
    print('ERROR')

  return dumps(query_to_chart(chart_data))