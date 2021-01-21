from flask import Blueprint, jsonify, request
from bson.json_util import dumps
from configs.mongodb import mongo
from application.globals import KANWIL

filters_blueprint = Blueprint('filters_blueprint', __name__)


@filters_blueprint.route('/jenis_belanja', methods=['GET'])
def perjenis_belanja():
  perjenis_belanja = []

  try:
    perjenis_belanja = list(mongo.db.jenis_belanja.find(KANWIL))
  except:
    print('ERROR')

  return dumps(perjenis_belanja)


@filters_blueprint.route('/perkabupaten', methods=['GET'])
def perkabupaten():
  perkabupaten = []

  try:
    perkabupaten = list(mongo.db.per_kabupaten.aggregate([
    {
      '$lookup': {
        'from': 'ref_kabupaten',
        'localField': 'kdkabkota',
        'foreignField': 'kdkabkota',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perkabupaten)


@filters_blueprint.route('/perkppn', methods=['GET'])
def perkppn():
  perkppn = []

  try:
    perkppn = list(mongo.db.per_kppn.aggregate([
    {
      '$lookup': {
        'from': 'ref_kppn',
        'localField': 'kdkppn',
        'foreignField': 'kdkppn',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perkppn)


@filters_blueprint.route('/paguminus', methods=['GET'])
def paguminus():
  paguminus = []

  try:
    paguminus = mongo.db.paguminus.find({})
  except:
    print('ERROR')

  return dumps(paguminus)