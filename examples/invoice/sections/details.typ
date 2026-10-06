#let data = json(sys.inputs.data_file)
#let items = data.at("items", default: ())

#table(
  columns: (1fr, auto, auto),
  inset: 8pt,
  [Позиция], [Количество], [Цена],
  ..items.map(item => (
    item.description,
    str(item.quantity),
    str(item.price),
  )).flatten(),
)

Сформировано автоматически из Git-проекта и JSON.
