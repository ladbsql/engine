from db import LaDB

usuario = {
  "id": 1,
  "nombre": "Juan",
  "apellido": "Perez",
  "edad": 25
}

mi_db = LaDB('pruebas.ldb')
mi_db.insert_in_table('usuarios', usuario)