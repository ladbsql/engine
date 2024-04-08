import json

class LaDB:
  def __init__(self, filename='datos.ldb'):
    self.filename = filename
    self.filepath = f'./databases/{self.filename}'
    self.data = self.load_data()
    if not self.data:
      self.data = {}
      self.save_data()

  def load_data(self):
        try:
            with open(self.filepath, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"Archivo {self.filepath} no encontrado.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON en {self.filepath}: {e}")
            return {}

  def save_data(self):
    with open(self.filepath, 'w') as file:
      json.dump(self.data, file, indent=4)

  def list_tables(self):
        return list(self.data.keys())

  def create_table(self, table_name, primary_key = None, foreign_keys = None):
    if table_name not in self.data:
      self.data[table_name] = {"records": [], "primary_key": primary_key, "foreign_keys": foreign_keys or {}}
      self.save_data()
      return True
    return False

  def insert_in_table(self, table_name, record):
    if table_name in self.data:
      table_info = self.data[table_name]
      pk = table_info["primary_key"]

      # Verificar unicidad de la PK
      if pk and any(r[pk] == record[pk] for r in table_info["records"]):
        print(f"Error: duplicated in the primary key '{pk}'.")
        return False

      # Verificar claves foraneas
      for fk, ref in table_info.get("foreign_keys", {}).items():
        ref_table, ref_pk = ref
        if not any(r[ref_pk] == record[fk] for r in self.data[ref_table]["records"]):
          print(f"Error: Foreign key constraint failed for '{fk}' referencing '{ref_table}.{ref_pk}'")
          return False
      table_info["records"].append(record)
      self.save_data()
      return True
    return False

  def select(self, table_name, fields=None, join=None, where=None, order_by=None, limit=None):
    records = self.data.get(table_name, {}).get("records", [])

    if where:
      records = list(filter(where, records))

    results = []

    for record in records:
      selected_record = {field: record.get(field) for field in fields} if fields else record.copy()

      if join:
        for join_table, (fk_field, pk_field) in join.items():
          related_record = next((r for r in self.data[join_table]["records"] if r[pk_field] == record.get(fk_field, None)), None)
          if related_record:
            selected_record.update({"joined_" + join_table: related_record})

      results.append(selected_record)

    if order_by:
      field, direction = order_by
      results = sorted(results, key=lambda x: x.get(field), reverse=(direction == "desc"))

    if limit is not None:
      results = results[:limit]

    return results
  
  def get_tables_info(self):
        tables_info = []
        for table_name, table_data in self.data.items():
            # Asumiendo que `table_data` es un diccionario que incluye una clave 'records'
            record_count = len(table_data.get('records', []))
            tables_info.append({
                'name': table_name,
                'record_count': record_count
            })
        return tables_info

  def drop(self, table_name):
    if table_name in self.data:
      del self.data[table_name]
      self.save_data()
      print(f"Table '{table_name}' has been dropped successfully.")
    else:
      print(f"Error: Table '{table_name} does not exist.'")

  def delete(self, table_name, condition):
    if table_name not in self.data:
      print(f"Error: Table '{table_name}' not found.")
      return False
    records = self.data[table_name]["records"]
    self.data[table_name]["records"] = [record for record in records if not condition(record)]

    self.save_data()
    print("Records deleted succefully")
    return True