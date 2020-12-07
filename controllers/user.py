from flask import Blueprint, jsonify, request
from bson.json_util import dumps
from configs.mongodb import mongo

users_blueprint = Blueprint('users_blueprint', __name__)

@users_blueprint.route('/user', methods=['POST'])
def user():
  user = {
    'kode_kanwil': '',
    'nama_kanwil': ''
  }

  user_data = list(mongo.db.users.find({'kode_kanwil': str(request.get_json()['kdkanwil'])}))

  if len(user_data) == 0:
    user['kode_kanwil'] = str(request.get_json()['kdkanwil'])
    user['nama_kanwil'] = str(request.get_json()['nama'])

    mongo.db.users.insert_one(user)
  else:
    user = user_data

  return dumps(user)