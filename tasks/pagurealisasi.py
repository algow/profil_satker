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
  df.replace({'\'': ''}, regex=True, inplace=True)

  df['kdkanwil'] = df['kdkanwil'].apply(str)
  df['kdsatker'] = df['kdsatker'].apply(str)
  df['kdakun'] = df['kdakun'].apply(str)
  df['kdkabkota'] = df['kdkabkota'].apply(str)
  # df['realisasi'].replace({'\.': ''}, regex=True, inplace=True)
  # df['pagu'].replace({'\.': ''}, regex=True, inplace=True)
  # df['blokir'].replace({'\.': ''}, regex=True, inplace=True)
  # df['realisasi'] = df['realisasi'].apply(int)
  # df['pagu'] = df['realisasi'].apply(int)
  # df['blokir'] = df['realisasi'].apply(int)
  
  df['pagu'].replace(0, 1, inplace=True)
  df['persentase'] = round(df['realisasi'] / df['pagu'], 4)

  # paguminus_df = df[df['persentase'] > 1]
  # suspense_df = df[df['kdkabkota'] == 'ZZZZ']
  realisasi_minus_df = df[df['realisasi'] < 0]

  total = {
    'kdkanwil': df.at[0, 'kdkanwil'],
    'pagu': int(str(df['pagu'].sum())),
    'realisasi': int(str(df['realisasi'].sum()))
  }
  
  persatker_df = df.groupby(['kdsatker', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()

  perdept_df = df.groupby(['kddept', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perdept_df['persentase'] = round(perdept_df['realisasi'] / perdept_df['pagu'], 4)

  perdekon_df = df.groupby(['kddekon', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perdekon_df['persentase'] = round(perdekon_df['realisasi'] / perdekon_df['pagu'], 4)

  persdana_df = df.groupby(['kdsdana', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  persdana_df['persentase'] = round(persdana_df['realisasi'] / persdana_df['pagu'], 4)

  perkat_out_df = df.groupby(['kat_out', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perkat_out_df['persentase'] = round(perkat_out_df['realisasi'] / perkat_out_df['pagu'], 4)

  perfungsi_df = df.groupby(['kdfungsi', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perfungsi_df['persentase'] = round(perfungsi_df['realisasi'] / perfungsi_df['pagu'], 4)

  perprogram_df = df.groupby(['kdprogram', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perprogram_df['persentase'] = round(perprogram_df['realisasi'] / perprogram_df['pagu'], 4)

  perkegiatan_df = df.groupby(['kdgiat', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perkegiatan_df['persentase'] = round(perkegiatan_df['realisasi'] / perkegiatan_df['pagu'], 4)

  peroutput_df = df.groupby(['kdoutput', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  peroutput_df['persentase'] = round(peroutput_df['realisasi'] / peroutput_df['pagu'], 4)

  perkabupaten_df = df.groupby(['kdkabkota', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perkabupaten_df['persentase'] = round(perkabupaten_df['realisasi'] / perkabupaten_df['pagu'], 4)

  perkppn_df = df.groupby(['kdkppn', 'kdkanwil'])[['pagu', 'blokir', 'realisasi']].sum().reset_index()
  perkppn_df['persentase'] = round(perkppn_df['realisasi'] / perkppn_df['pagu'], 4)

  perjenis_belanja = df.groupby([df['kdakun'].str[:2], 'kdkanwil'])[['kdkanwil', 'kdakun', 'pagu', 'blokir', 'realisasi']].sum().reset_index()
  perjenis_belanja['persentase'] = round(perjenis_belanja['realisasi'] / perjenis_belanja['pagu'], 4)

  refsatker_df = df.groupby(['kdsatker']).first()[['nmsatker', 'kdkanwil', 'kdkppn', 'kddept', 'kdkabkota']].reset_index()
  refkabupaten_df = df.groupby(['kdkabkota']).first()[['nmkabkota', 'kdkanwil', 'kdkppn']].reset_index()
  refkppn_df = df.groupby(['kdkppn']).first()[['nmkppn', 'kdkanwil']].reset_index()
  refdept_df = df.groupby(['kddept']).first()[['nmdept', 'kdkanwil']].reset_index()
  refdekon_df = df.groupby(['kddekon']).first()[['nmdekon', 'kdkanwil']].reset_index()
  refsdana_df = df.groupby(['kdsdana']).first()[['nmsdana', 'kdkanwil']].reset_index()
  refkat_out_df = df.groupby(['kat_out']).first()[['uraian_kat_out', 'kdkanwil']].reset_index()
  reffungsi_df = df.groupby(['kdfungsi']).first()[['nmfungsi', 'kdkanwil']].reset_index()
  refprogram_df = df.groupby(['kdprogram']).first()[['nmprogram', 'kdkanwil']].reset_index()
  refkegiatan_df = df.groupby(['kdgiat']).first()[['nmgiat', 'kdkanwil']].reset_index()
  refoutput_df = df.groupby(['kdoutput']).first()[['nmoutput', 'kdkanwil']].reset_index()

  perjenis_belanja['jenis'] = perjenis_belanja['kdakun'].apply(lambda x: 'Belanja Pegawai' if x == '51' else ('Belanja Barang' if x == '52' else ('Belanja Modal' if x == '53' else ('Belanja Bansos' if x == '57' else ('DAK Fisik' if x == '63' else ('DAK Nonfisik' if x == '65' else 'Dana Desa'))))))

  pagurealisasi_exist = mongo.db.pagurealisasi.find(KANWIL)

  if len(list(pagurealisasi_exist)) > 0:
    mongo.db.totals.delete_many(KANWIL)
    mongo.db.pagurealisasi.delete_many(KANWIL)
    mongo.db.jenis_belanja.delete_many(KANWIL)
    # mongo.db.paguminus.delete_many(KANWIL)
    # mongo.db.suspense.delete_many(KANWIL)
    mongo.db.realisasi_minus.delete_many(KANWIL)
    mongo.db.per_satker.delete_many(KANWIL)
    mongo.db.per_kabupaten.delete_many(KANWIL)
    mongo.db.per_kppn.delete_many(KANWIL)
    mongo.db.per_fungsi.delete_many(KANWIL)
    mongo.db.per_dept.delete_many(KANWIL)
    mongo.db.per_dekon.delete_many(KANWIL)
    mongo.db.per_sdana.delete_many(KANWIL)
    mongo.db.per_kat_out.delete_many(KANWIL)
    mongo.db.per_program.delete_many(KANWIL)
    mongo.db.per_kegiatan.delete_many(KANWIL)
    mongo.db.per_output.delete_many(KANWIL)

    mongo.db.ref_satker.delete_many(KANWIL)
    mongo.db.ref_kabupaten.delete_many(KANWIL)
    mongo.db.ref_kppn.delete_many(KANWIL)
    # mongo.db.ref_dept.delete_many(KANWIL)
    mongo.db.ref_dekon.delete_many(KANWIL)
    mongo.db.ref_sdana.delete_many(KANWIL)
    mongo.db.ref_kat_out.delete_many(KANWIL)
    mongo.db.ref_fungsi.delete_many(KANWIL)
    mongo.db.ref_program.delete_many(KANWIL)
    mongo.db.ref_kegiatan.delete_many(KANWIL)
    mongo.db.ref_output.delete_many(KANWIL)

  mongo.db.totals.insert_one(total)
  mongo.db.pagurealisasi.insert_many(df.to_dict(orient='records'))
  mongo.db.jenis_belanja.insert_many(perjenis_belanja.to_dict(orient='records'))
  
  # mongo.db.paguminus.insert_many(paguminus_df.to_dict(orient='records'))
  # mongo.db.suspense.insert_many(suspense_df.to_dict(orient='records'))
  mongo.db.realisasi_minus.insert_many(realisasi_minus_df.to_dict(orient='records'))
  
  mongo.db.per_satker.insert_many(persatker_df.to_dict(orient='records'))
  mongo.db.per_kabupaten.insert_many(perkabupaten_df.to_dict(orient='records'))
  mongo.db.per_kppn.insert_many(perkppn_df.to_dict(orient='records'))
  mongo.db.per_fungsi.insert_many(perfungsi_df.to_dict(orient='records'))
  
  mongo.db.per_dept.insert_many(perdept_df.to_dict(orient='records'))
  mongo.db.per_dekon.insert_many(perdekon_df.to_dict(orient='records'))
  mongo.db.per_sdana.insert_many(persdana_df.to_dict(orient='records'))
  mongo.db.per_kat_out.insert_many(perkat_out_df.to_dict(orient='records'))
  mongo.db.per_program.insert_many(perprogram_df.to_dict(orient='records'))
  mongo.db.per_kegiatan.insert_many(perkegiatan_df.to_dict(orient='records'))
  mongo.db.per_output.insert_many(peroutput_df.to_dict(orient='records'))

  mongo.db.ref_satker.insert_many(refsatker_df.to_dict(orient='records'))
  mongo.db.ref_kabupaten.insert_many(refkabupaten_df.to_dict(orient='records'))
  mongo.db.ref_kppn.insert_many(refkppn_df.to_dict(orient='records'))
  mongo.db.ref_dept.insert_many(refdept_df.to_dict(orient='records'))
  mongo.db.ref_dekon.insert_many(refdekon_df.to_dict(orient='records'))
  mongo.db.ref_sdana.insert_many(refsdana_df.to_dict(orient='records'))
  mongo.db.ref_kat_out.insert_many(refkat_out_df.to_dict(orient='records'))
  mongo.db.ref_fungsi.insert_many(reffungsi_df.to_dict(orient='records'))
  mongo.db.ref_program.insert_many(refprogram_df.to_dict(orient='records'))
  mongo.db.ref_kegiatan.insert_many(refkegiatan_df.to_dict(orient='records'))
  mongo.db.ref_output.insert_many(refoutput_df.to_dict(orient='records'))