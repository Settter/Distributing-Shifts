from flask import Flask, jsonify, request, abort

from db_methods import init_database

app = Flask(__name__)

db_name = 'main'
db_password = '123'

username = 'admin'
password = 'admin'

@app.route('/shifts/api/v1.0/first_timee_workers', methods=['POST'])
def add_workers():
    workers = {}
    if not request.json or not 'title' in request.json:
        abort(400)
    else:
        workers = request.json
        print(workers)
    return jsonify({'tasks': 'tasks'})


@app.route('/todo/api/v1.0/tasks', methods=['POST'])
def create_task():
    if not request.json or not 'title' in request.json:
        abort(400)
    return jsonify({'task': 'task'})


@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):


if __name__ == '__main__':
    init_database()
    app.run(debug=True)
