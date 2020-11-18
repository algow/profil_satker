from application import factory
from application import celery

if __name__ == '__main__':
  flask_app = factory.create_app(celery=celery)
  flask_app.run()