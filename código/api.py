import flask
import logging
import psycopg2
import jwt
import datetime

app = flask.Flask(__name__)
app.config['SECRET_KEY'] = 'tempsecretkey'
TEMP_TOKEN_LIST = []

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}

class loginError(Exception):
    def __init__(self, message='Failed to create user token'):
        super(loginError, self).__init__(message)


class InvalidAuthenticationException(Exception):
    def __init__(self, message='User not registered'):
        super(InvalidAuthenticationException, self).__init__(message)


class InsufficientPrivilegesException(Exception):
    def __init__(self, message='User must be administrator'):
        super(InsufficientPrivilegesException, self).__init__(message)


##########################################################
## DATABASE ACCESS
##########################################################
def db_connection():
    db = psycopg2.connect(
        user='projuser',
        password='projuser',
        host='127.0.0.1',
        port='5432',
        database='dbproj'
    )
    return db


##########################################################
## ENDPOINTS
##########################################################
@app.route('/')
def landing_page():
    return """
    Hello World (Python Native)!  <br/>
    <br/>
    Check the sources for instructions on how to use the endpoints!<br/>
    <br/>
    BD 2022 Team<br/>
    <br/>
    """


##
## Demo GET
##
## Obtain department with ndep <ndep>
##
## To use it, access:
##
## http://localhost:8080/departments/10
##
@app.route('/departments/<ndep>/', methods=['GET'])
def get_department(ndep):
    logger.info('GET /departments/<ndep>')
    logger.debug(f'ndep: {ndep}')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT ndep, nome, local FROM dep where ndep = %s', (ndep,))
        rows = cur.fetchall()
        row = rows[0]
        logger.debug('GET /departments/<ndep> - parse')
        logger.debug(row)
        content = {'ndep': int(row[0]), 'nome': row[1], 'localidade': row[2]}
        response = {'status': StatusCodes['success'], 'results': content}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /departments/<ndep> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
## Demo PUT
##
## Update a department based on a JSON payload
##
## To use it, you need to use postman or curl:
##
## curl -X PUT http://localhost:8080/departments/ -H 'Content-Type: application/json' -d '{'ndep': 69, 'localidade': 'Porto'}'
##
@app.route('/departments/<ndep>', methods=['PUT'])
def update_departments(ndep):
    logger.info('PUT /departments/<ndep>')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /departments/<ndep> - payload: {payload}')

    # do not forget to validate every argument, e.g.,:
    if 'localidade' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'localidade is required to update'}
        return flask.jsonify(response)

    # parameterized queries, good for security and performance
    statement = 'UPDATE dep SET local = %s WHERE ndep = %s'
    values = (payload['localidade'], ndep)

    try:
        res = cur.execute(statement, values)
        response = {'status': StatusCodes['success'], 'results': f'Updated: {cur.rowcount}'}
        # commit the transaction
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/users/', methods=['GET'])
def get_all_users():
    logger.info('GET /users')

    user_token = flask.request.headers.get('Authorization').split(' ')[1]
    print(user_token)

    conn = db_connection()
    cur = conn.cursor()

    try:
        if not user_token.isnumeric():
            raise InsufficientPrivilegesException()

        admin_validation = 'SELECT * FROM admins WHERE users_user_id = %s'
        admin_validation = "DO $$" \
                           "DECLARE" \
                           "    v_admin admin%ROWTYPE" \
                           "BEGIN" \
                           "    SELECT * INTO STRICT v_admin FROM admins WHERE users_user_id = %s" \
                           "EXCEPTION" \
                           "    WHEN no_data_found THEN" \
                           "        RAISE EXCEPTION 'User is not administrator''" \
                           "END;" \
                           "$$"
        # admin_validation =  "SELECT * FROM admins WHERE users_user_id = %s"

        cur.execute(admin_validation, [int(user_token)])
        login_result = cur.fetchone()

        if login_result is None:
            raise InsufficientPrivilegesException()

        cur.execute('SELECT user_id, username, password FROM users')  # FIXME: password
        rows = cur.fetchall()

        logger.debug('GET /users - parse')
        Results = []
        for row in rows:
            logger.debug(row)
            content = {'user_id': row[0], 'username': row[1], 'password': row[2]}
            Results.append(content)  # appending to the payload to be returned
        response = {'status': StatusCodes['success'], 'results': Results}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /users - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/users/', methods=['POST'])
def register_user():
    logger.info('POST /users')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /users - payload: {payload}')

    if 'user_id' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'user_id not in payload'}
        return flask.jsonify(response)

    # parameterized queries, good for security and performance
    statement = 'INSERT INTO users (user_id, username, password) VALUES (%s, %s, %s)'
    values = (payload['user_id'], payload['username'], payload['password'])

    try:
        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Inserted user {payload["username"]}'}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /users - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/users/', methods=['PUT'])
def login_user():
    logger.info('PUT /users')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /users - payload: {payload}')

    if 'username' not in payload or 'password' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'username and password are required for login'}
        return flask.jsonify(response)

    statement = 'SELECT user_id FROM users WHERE username = %s AND password = %s FOR UPDATE'
    values = (payload['username'], payload['password'])

    try:
        cur.execute(statement, values)
        row = cur.fetchone()

        if row is not None:
            auth_token = jwt.encode({'user': row[1],
                                     'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=10)},
                                    app.config['SECRET_KEY'])

            TEMP_TOKEN_LIST.append(auth_token.split()[2])
            try:
                decode_attempt = jwt.decode(auth_token, app.config['SECRET_KEY'])
            except jwt.exceptions.InvalidTokenError as e:
                raise loginError()
            '''
            user_id = row[0]
            statement = 'UPDATE users SET token = %s WHERE user_id = %s'
            values = (auth_token.split()[2], user_id)
            cur.execute(statement, values)
            '''

        else:
            raise InvalidAuthenticationException()

        response = {'status': StatusCodes['success'], 'token': auth_token}  # TODO: JWT authent
        # commit the transaction
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


if __name__ == '__main__':
    # set up logging
    logging.basicConfig(filename='log_file.log')
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s]:  %(message)s', '%H:%M:%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    host = '127.0.0.1'
    port = 8080

    app.run(host=host, debug=True, threaded=True, port=port)
    logger.info(f'API online: http://{host}:{port}')
