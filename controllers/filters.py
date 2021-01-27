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


@filters_blueprint.route('/perorganisasi', methods=['GET'])
def perorganisasi():
  perorganisasi = []

  try:
    perorganisasi = list(mongo.db.per_dept.aggregate([
    {
      '$lookup': {
        'from': 'ref_dept',
        'localField': 'kddept',
        'foreignField': 'kddept',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perorganisasi)


@filters_blueprint.route('/perdekon', methods=['GET'])
def perdekon():
  perdekon = []

  try:
    perdekon = list(mongo.db.per_dekon.aggregate([
    {
      '$lookup': {
        'from': 'ref_dekon',
        'localField': 'kddekon',
        'foreignField': 'kddekon',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perdekon)


@filters_blueprint.route('/perfungsi', methods=['GET'])
def perfungsi():
  perfungsi = []

  try:
    perfungsi = list(mongo.db.per_fungsi.aggregate([
    {
      '$lookup': {
        'from': 'ref_fungsi',
        'localField': 'kdfungsi',
        'foreignField': 'kdfungsi',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perfungsi)


@filters_blueprint.route('/perprogram', methods=['GET'])
def perprogram():
  perprogram = []

  try:
    perprogram = list(mongo.db.per_program.aggregate([
    {
      '$lookup': {
        'from': 'ref_program',
        'localField': 'kdprogram',
        'foreignField': 'kdprogram',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perprogram)


@filters_blueprint.route('/perkegiatan', methods=['GET'])
def perkegiatan():
  perkegiatan = []

  try:
    perkegiatan = list(mongo.db.per_kegiatan.aggregate([
    {
      '$lookup': {
        'from': 'ref_kegiatan',
        'localField': 'kdgiat',
        'foreignField': 'kdgiat',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perkegiatan)


@filters_blueprint.route('/peroutput', methods=['GET'])
def peroutput():
  peroutput = []

  try:
    peroutput = list(mongo.db.per_output.aggregate([
    {
      '$lookup': {
        'from': 'ref_output',
        'localField': 'kdoutput',
        'foreignField': 'kdoutput',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(peroutput)


@filters_blueprint.route('/persumberdana', methods=['GET'])
def persumberdana():
  persumberdana = []

  try:
    persumberdana = list(mongo.db.per_sdana.aggregate([
    {
      '$lookup': {
        'from': 'ref_sdana',
        'localField': 'kdsdana',
        'foreignField': 'kdsdana',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(persumberdana)


@filters_blueprint.route('/perkatoutput', methods=['GET'])
def perkatoutput():
  perkatoutput = []

  try:
    perkatoutput = list(mongo.db.per_kat_out.aggregate([
    {
      '$lookup': {
        'from': 'ref_kat_out',
        'localField': 'kat_out',
        'foreignField': 'kat_out',
        'as': 'referensi'
      }
    }]))
  except:
    print('ERROR')

  return dumps(perkatoutput)

# @filters_blueprint.route('/paguminus', methods=['GET'])
# def paguminus():
#   paguminus = []

#   try:
#     paguminus = mongo.db.paguminus.find({})
#   except:
#     print('ERROR')

#   return dumps(paguminus)