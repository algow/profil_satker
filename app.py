from application import factory
from application import celery
from tasks import columns_name

if __name__ == '__main__':
  flask_app = factory.create_app(celery=celery)
  flask_app.run(host='0.0.0.0')