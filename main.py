from flask import Flask, request, jsonify
from flask_cors import CORS
from db import LaDB
import os


app = Flask(__name__)
CORS(app)

@app.route('/create_database', methods=['POST'])
def create_database():
    data = request.json
    filename = data.get('filename')

    if not filename.endswith('.ldb'):
        filename += '.ldb'

    db_path = f'./databases/{filename}'
    if os.path.exists(db_path):
        return jsonify({"message": f"Database '{filename}' already exists."}), 400

    db = LaDB(filename)
    return jsonify({"message": f"Database '{filename}' created successfully."}), 201

@app.route('/delete_database/<dbname>', methods=['DELETE'])
def delete_database(dbname):
    if not dbname.endswith('.ldb'):
        dbname += '.ldb'

    db_path = f'./databases/{dbname}'

    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found"}), 404

    try:
        os.remove(db_path)
        return jsonify({"message": f"Database '{dbname}' deleted successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/list_databases', methods=['GET'])
def list_databases():
    databases = [f for f in os.listdir('./databases') if os.path.isfile(os.path.join('./databases', f))]
    return jsonify({"databases": databases}), 200

@app.route('/<dbname>/list_tables', methods=['GET'])
def list_tables(dbname):
    if not dbname.endswith('.ldb'):
        dbname += '.ldb'
    db_path = f'./databases/{dbname}'  # Asegúrate de que este path es correcto.

    # Debugging: Imprime la ruta para verificar que es correcta
    print(f"Cargando datos desde: {db_path}")

    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found"}), 404

    db = LaDB(dbname)  # Aquí pasas dbname pero LaDB espera la ruta completa, ajusta según tu implementación
    tables = db.list_tables()
    print(tables)  # Debugging: Verifica qué tablas se están listando

    return jsonify({"tables": tables}), 200

@app.route('/<dbname>/get_tables_info', methods=['GET'])
def get_tables_info(dbname):
    if not dbname.endswith('.ldb'):
        dbname = f"{dbname}.ldb"
    db_path = f'./databases/{dbname}'

    if not os.path.exists(db_path):
        return jsonify({"error": "Database not found"}), 404

    db = LaDB(dbname)
    tables_info = db.get_tables_info()
    return jsonify({"tables": tables_info}), 200

@app.route('/<dbname>/create_table', methods=['POST'])
def insert_record(dbname, table_name, record):
    db = LaDB(dbname)
    if db.insert_in_table(table_name, record):
        return jsonify({"message": "Record inserted successfully."}), 201
    return jsonify({"message": "Error inserting record."}), 400

if __name__ == '__main__':
    app.run(debug=True)