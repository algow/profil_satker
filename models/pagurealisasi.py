from configs.mongodb import mongo

def pagurealisasi():
  mongo.db.create_collection('pagurealisasi', validator = {
    '$jsonSchema': {
      'bsonType': 'object',
      'properties': {
        'kode_kanwil': {
          'bsonType': 'string'
        },
        'kode_kppn': {
          'bsonType': 'string'
        },
        'kppn': {
          'bsonType': 'string'
        },
        'kode_satker': {
          'bsonType': 'string'
        },
        'nama_satker': {
          'bsonType': 'string'
        },
        'kode_ba': {
          'bsonType': 'string'
        },
        'akun': {
          'bsonType': 'string'
        },
        'kode_kabupaten': {
          'bsonType': 'string'
        },
        'kabupaten': {
          'bsonType': 'string'
        },
        'dipa': {
          'bsonType': 'int'
        },
        'blokir': {
          'bsonType': 'int'
        },
        'outs_kontrak': {
          'bsonType': 'int'
        },
        'realisasi': {
          'bsonType': 'int'
        }
      }
    }
  })