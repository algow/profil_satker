from flask import current_app
import pandas as pd
from configs.mongodb import mongo
from application import celery
from application.globals import KANWIL

@celery.task()
def store_pagu(filename):
  df = pd.read_excel(filename, header=3)
  df.columns = ['kode_kanwil', 'kode_kppn', 'kppn', 'kode_satker', 'nama_satker', 'kode_ba', 'akun', 'kode_kabupaten', 'kabupaten', 'dipa', 'blokir', 'outs_kontrak', 'realisasi']
  df['dipa'].replace(0, 1, inplace=True)
  df['persentase'] = round(df['realisasi'] / df['dipa'], 4)

  paguminus_df = df[df['persentase'] > 1]

  persatker_df = df.groupby(['kode_satker', 'kode_kanwil'])['dipa', 'blokir', 'outs_kontrak', 'realisasi'].sum().reset_index()
  perkabupaten_df = df.groupby(['kode_kabupaten', 'kode_kanwil'])['dipa', 'blokir', 'outs_kontrak', 'realisasi'].sum().reset_index()
  perkppn_df = df.groupby(['kode_kppn', 'kode_kanwil'])['dipa', 'blokir', 'outs_kontrak', 'realisasi'].sum().reset_index()

  refsatker_df = df.groupby(['kode_satker']).first()[['nama_satker', 'kode_kanwil', 'kode_kppn', 'kode_ba', 'kode_kabupaten']].reset_index()
  refkabupaten_df = df.groupby(['kode_kabupaten']).first()[['kabupaten', 'kode_kanwil', 'kode_kppn']].reset_index()
  refkppn_df = df.groupby(['kode_kppn']).first()[['kppn', 'kode_kanwil']].reset_index()

  pagurealisasi_exist = mongo.db.pagurealisasi.find(KANWIL)
  
  if len(list(pagurealisasi_exist)) > 0:
    mongo.db.pagurealisasi.delete_many(KANWIL)
    mongo.db.paguminus.delete_many(KANWIL)
    mongo.db.per_satker.delete_many(KANWIL)
    mongo.db.per_kabupaten.delete_many(KANWIL)
    mongo.db.per_kppn.delete_many(KANWIL)
    mongo.db.ref_satker.delete_many(KANWIL)
    mongo.db.ref_kabupaten.delete_many(KANWIL)
    mongo.db.ref_kppn.delete_many(KANWIL)

  mongo.db.pagurealisasi.insert_many(df.to_dict(orient='records'))
  mongo.db.paguminus.insert_many(paguminus_df.to_dict(orient='records'))
  mongo.db.per_satker.insert_many(persatker_df.to_dict(orient='records'))
  mongo.db.per_kabupaten.insert_many(perkabupaten_df.to_dict(orient='records'))
  mongo.db.per_kppn.insert_many(perkppn_df.to_dict(orient='records'))
  mongo.db.ref_satker.insert_many(refsatker_df.to_dict(orient='records'))
  mongo.db.ref_kabupaten.insert_many(refkabupaten_df.to_dict(orient='records'))
  mongo.db.ref_kppn.insert_many(refkppn_df.to_dict(orient='records'))