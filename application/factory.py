from flask import Flask
import os
from flask_cors import CORS
from configs.mongodb import MONGO_URI, mongo
from models import init_db_collections
from .celery_util import init_celery

PKG_NAME = os.path.dirname(os.path.realpath(__file__)).split('/')[-1]

def create_app(app_name=PKG_NAME, **kwargs):
  app = Flask(app_name)
  CORS(app, resources={r'/*': {'origins': '*'}})

  app.config['MONGO_URI'] = MONGO_URI
  mongo.init_app(app)
  # init_db_collections()

  if kwargs.get('celery'):
    init_celery(kwargs.get('celery'), app)
  
  from controllers.pagurealisasi import pagurealisasi_blueprint
  from controllers.persatker import persatker_blueprint
  from controllers.filters import filters_blueprint
  from controllers.chart import chart_blueprint
  from controllers.user import users_blueprint
  from controllers.anomaly import anomalies_blueprint

  app.register_blueprint(pagurealisasi_blueprint)
  app.register_blueprint(persatker_blueprint)
  app.register_blueprint(filters_blueprint)
  app.register_blueprint(chart_blueprint)
  app.register_blueprint(users_blueprint)
  app.register_blueprint(anomalies_blueprint)

  return app