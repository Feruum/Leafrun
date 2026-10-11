#let data = json("/data.json")
#set page(paper: "a4", margin: 22mm)
#set text(font: "Libertinus Serif", size: 11pt)
#image("assets/logo.svg", width: 12mm)

= #data.at("title", default: "Invoice")

Customer: #data.at("customer", default: "Example Company")

#include "sections/details.typ"
