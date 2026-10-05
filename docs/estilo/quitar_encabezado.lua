-- La portada reemplaza al encabezado del Markdown (título, datos de la materia, versión y autores):
-- se quita todo lo que está antes del primer bloque de cita.
function Pandoc(doc)
  local out, started = {}, false
  for _, b in ipairs(doc.blocks) do
    if b.t == "BlockQuote" then started = true end
    if started then table.insert(out, b) end
  end
  doc.blocks = out
  return doc
end
