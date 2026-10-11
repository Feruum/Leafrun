#let data = json("/data.json")
#let items = data.at("items", default: ())

#table(
  columns: (1fr, auto, auto),
  inset: 8pt,
  [Item], [Quantity], [Price],
  ..items.map(item => (
    item.description,
    str(item.quantity),
    str(item.price),
  )).flatten(),
)

Generated automatically from a local project and JSON.
