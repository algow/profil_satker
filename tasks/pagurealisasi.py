from flask import current_app
import pandas as pd
from configs.mongodb import mongo
from application import celery
from application.globals import KANWIL
import json
# from .columns_name import InputColumn

@celery.task()
def store_pagu(filename):
  df = pd.read_excel(filename, header=4, engine='openpyxl')
  df.columns = ['no','kddept','nmdept','kddekon','nmdekon','kdkabkota','nmkabkota','kdkanwil','kdkppn','nmkppn','kdsatker','nmsatker','kdfungsi','nmfungsi','kdprogram','nmprogram','kdgiat','nmgiat','kdoutput','nmoutput','kdakun','nmakun','kdsdana','nmsdana','kat_out','uraian_kat_out','pagu','realisasi','blokir']

  df['kdkanwil'] = df['kdkanwil'].apply(str)
  df['kdsatker'] = df['kdsatker'].apply(str)
  df['kdakun'] = df['kdakun'].apply(str)
  df['kdkabkota'] = df['kdkabkota'].apply(str)

  df['pagu'].replace(0, 1, inplace=True)
  df['persentase'] = round(df['realisasi'] / df['pagu'], 4)

  paguminus_df = df[df['persentase'] > 1]
  # suspense_df = df[df['kdkabkota'] == 'ZZZZ']
  realisasi_minus_df = df[df['realisasi'] < 0]

  total = {
    'kdkanwil': df.at[0, 'kdkanwil'],
    'pagu': int(str(df['pagu'].sum())),
    'realisasi': int(str(df['realisasi'].sum()))
  }

  persatker_df = df.groupby(['kdsatker', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()

  perkabupaten_df = df.groupby(['kdkabkota', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perkabupaten_df['persentase'] = round(perkabupaten_df['realisasi'] / perkabupaten_df['pagu'], 4)

  perkppn_df = df.groupby(['kdkppn', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perkppn_df['persentase'] = round(perkppn_df['realisasi'] / perkppn_df['pagu'], 4)

  perfungsi_df = df.groupby(['kdfungsi', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()

  perjenis_belanja = df.groupby([df['kdakun'].str[:2], 'kdkanwil'])[['kdkanwil', 'kdakun', 'pagu', 'blokir', 'realisasi']].sum().reset_index()

  refsatker_df = df.groupby(['kdsatker']).first()[['nmsatker', 'kdkanwil', 'kdkppn', 'kddept', 'kdkabkota']].reset_index()
  refkabupaten_df = df.groupby(['kdkabkota']).first()[['nmkabkota', 'kdkanwil', 'kdkppn']].reset_index()
  refkppn_df = df.groupby(['kdkppn']).first()[['nmkppn', 'kdkanwil']].reset_index()

  perjenis_belanja['jenis'] = perjenis_belanja['kdakun'].apply(lambda x: 'Belanja Pegawai' if x == '51' else ('Belanja Barang' if x == '52' else ('Belanja Modal' if x == '53' else ('Belanja Bansos' if x == '57' else ('DAK Fisik' if x == '63' else ('DAK Nonfisik' if x == '65' else 'Dana Desa'))))))

  pagurealisasi_exist = mongo.db.pagurealisasi.find(KANWIL)

  if len(list(pagurealisasi_exist)) > 0:
    mongo.db.totals.delete_many(KANWIL)
    mongo.db.pagurealisasi.delete_many(KANWIL)
    mongo.db.jenis_belanja.delete_many(KANWIL)
    mongo.db.paguminus.delete_many(KANWIL)
    # mongo.db.suspense.delete_many(KANWIL)
    mongo.db.realisasi_minus.delete_many(KANWIL)
    mongo.db.per_satker.delete_many(KANWIL)
    mongo.db.per_kabupaten.delete_many(KANWIL)
    mongo.db.per_kppn.delete_many(KANWIL)
    mongo.db.ref_satker.delete_many(KANWIL)
    mongo.db.ref_kabupaten.delete_many(KANWIL)
    mongo.db.ref_kppn.delete_many(KANWIL)

  mongo.db.totals.insert_one(total)
  mongo.db.pagurealisasi.insert_many(df.to_dict(orient='records'))
  mongo.db.jenis_belanja.insert_many(perjenis_belanja.to_dict(orient='records'))
  mongo.db.paguminus.insert_many(paguminus_df.to_dict(orient='records'))
  # mongo.db.suspense.insert_many(suspense_df.to_dict(orient='records'))
  mongo.db.realisasi_minus.insert_many(realisasi_minus_df.to_dict(orient='records'))
  mongo.db.per_satker.insert_many(persatker_df.to_dict(orient='records'))
  mongo.db.per_kabupaten.insert_many(perkabupaten_df.to_dict(orient='records'))
  mongo.db.per_kppn.insert_many(perkppn_df.to_dict(orient='records'))
  mongo.db.ref_satker.insert_many(refsatker_df.to_dict(orient='records'))
  mongo.db.ref_kabupaten.insert_many(refkabupaten_df.to_dict(orient='records'))
  mongo.db.ref_kppn.insert_many(refkppn_df.to_dict(orient='records'))