def query_to_chart(elements):
  chart = {
    'label': [],
    'data': []
  }

  for element in elements:
    chart['label'].append(element['nama_kabupaten'][0])
    chart['data'].append(element['realisasi'])

  return chart