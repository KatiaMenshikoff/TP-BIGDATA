-- La portada reemplaza al encabezado del Markdown (título, datos de la materia, versión y autores):
-- se quita todo lo que está antes del primer bloque de cita o del primer título de sección (##).
function Pandoc(doc)
  local out, started = {}, false
  for _, b in ipairs(doc.blocks) do
    if b.t == "BlockQuote" or (b.t == "Header" and b.level == 2) then started = true end
    if started then table.insert(out, b) end
  end
  doc.blocks = out
  return doc
end
