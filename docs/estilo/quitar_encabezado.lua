-- La portada reemplaza al título y a la línea de datos del Markdown: se quitan el primer H1 y el párrafo que lo sigue.
local removed = 0
function Pandoc(doc)
  local out = {}
  for _, b in ipairs(doc.blocks) do
    if removed == 0 and b.t == "Header" and b.level == 1 then removed = 1
    elseif removed == 1 and b.t == "Para" then removed = 2
    else table.insert(out, b) end
  end
  doc.blocks = out
  return doc
end
