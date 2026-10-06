#let data = json(sys.inputs.data_file)
#set page(paper: "a4", margin: 22mm)
#set text(font: "Libertinus Serif", size: 11pt)
#image("assets/logo.svg", width: 12mm)

= #data.at("title", default: "Счёт")

Заказчик: #data.at("customer", default: "Пример компании")

#include "sections/details.typ"
