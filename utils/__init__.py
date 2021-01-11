def query_to_chart(elements):
  chart = {
    'label': [],
    'data': []
  }

  for element in elements:
    for key, value in element.items():
      if key == 'realisasi':
        chart['data'].append(value)
      else:
        if isinstance(value, list):
          chart['label'].append(value[0])
        else:
          chart['label'].append(value)

  return chart

def total_to_chart(elements):
  chart = {
    'label': ['sisa pagu', 'realisasi'],
    'data': []
  }

  chart['data'].append(elements[0]['pagu'])
  chart['data'].append(elements[0]['realisasi'])

  return chart