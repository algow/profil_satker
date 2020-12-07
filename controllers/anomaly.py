from flask import Blueprint, jsonify
from bson.json_util import dumps
from configs.mongodb import mongo

anomalies_blueprint = Blueprint('anonamlies_blueprint', __name__)


@anomalies_blueprint.route('/suspend', methods=['GET'])
def suspend():
  data = []

  try:
    pass
  except:
    pass