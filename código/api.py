import flask
import logging
import psycopg2
import jwt
import datetime

app = flask.Flask(__name__)
app.config['SECRET_KEY'] = 'my-32-character-ultra-secure-and-ultra-long-secret'
app.config['SESSION_COOKIE_NAME'] = 'our-db-project'

StatusCodes = {
    'success': 200,
    'bad_request': 400,
    'internal_error': 500
}


##########################################################
# EXCEPTIONS
##########################################################

class TokenError(Exception):
    def __init__(self, message='Invalid Authentication Token'):
        super(TokenError, self).__init__(message)


class TokenCreationError(Exception):
    def __init__(self, message='Failed to create user token'):
        super(TokenCreationError, self).__init__(message)


class InvalidAuthenticationException(Exception):
    def __init__(self, message='User not registered'):
        super(InvalidAuthenticationException, self).__init__(message)


class InsufficientPrivilegesException(Exception):
    def __init__(self, extra_msg, message='User must be administrator '):
        super(InsufficientPrivilegesException, self).__init__(message + extra_msg)


class ProductNotFound(Exception):
    def __init__(self, p_id, message='No product found with id: '):
        super(ProductNotFound, self).__init__(message + p_id)


class ParentQuestionNotFound(Exception):
    def __init__(self, question_id, message="Question not found: "):
        super(ParentQuestionNotFound, self).__init__(message + question_id)


##########################################################
# AUXILIARY FUNCTIONS
##########################################################


def get_user_id():
    try:
        header = flask.request.headers.get('Authorization')
        if header is None:
            raise jwt.exceptions.InvalidTokenError

        user_token = jwt.decode(header.split(' ')[1], app.config['SECRET_KEY'],
                                audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])

    except jwt.exceptions.InvalidTokenError as e:
        raise TokenError()

    return user_token['user']


def admin_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()
    try:
        user_id = get_user_id()

        admin_validation = 'select users_user_id ' \
                           'from admins ' \
                           'where users_user_id = %s'

        cur.execute(admin_validation, [user_id])

        if cur.fetchone() is None:
            raise InsufficientPrivilegesException(fail_msg)

    except TokenError as e:
        raise e

    except InsufficientPrivilegesException as e:
        raise e

    finally:
        if conn is not None:
            conn.close()


##########################################################
# DATABASE ACCESS
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
# TABLE COLUMNS
##########################################################

users_columns = ['user_id', 'username', 'password']
admins_columns = ['users_user_id']
buyers_columns = ['users_user_id', 'nif', 'home_addr']
sellers_columns = ['users_user_id', 'nif', 'shipping_addr']


##########################################################
# ENDPOINTS
##########################################################

@app.route('/users/', methods=['GET'])
def get_all_users():  # TODO: remover
    logger.info('GET /users')
    conn = db_connection()
    cur = conn.cursor()

    try:
        admin_check("to get user list")

        cur.execute('select * from users')
        rows = cur.fetchall()

        logger.debug('GET /users - parse')
        results = []
        for row in rows:
            logger.debug(row)
            content = {'user_id': row[0], 'username': row[1], 'password': row[2]}
            results.append(content)  # appending to the payload to be returned
        response = {'status': StatusCodes['success'], 'results': results}

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'GET /users - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /users - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/user/', methods=['POST'])
def register_user():
    logger.info('POST /users')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /users - payload: {payload}')

    if 'user_id' not in payload:
        response = {'status': StatusCodes['bad_request'], 'results': 'user_id not in payload'}
        return flask.jsonify(response)

    if 'username' not in payload:
        response = {'status': StatusCodes['bad_request'], 'results': 'username is required for user registry'}
        return flask.jsonify(response)

    if 'password' not in payload:
        response = {'status': StatusCodes['bad_request'], 'results': 'password is required for user registry'}
        return flask.jsonify(response)

    if 'type' not in payload or payload['type'] not in ['buyers', 'sellers', 'admins']:
        response = {'status': StatusCodes['bad_request'], 'results': 'user type is required for user registry: buyers, '
                                                                     'sellers or admins'}
        return flask.jsonify(response)

    if payload['type'] == 'buyers' or payload['type'] == 'sellers':
        if 'nif' not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': 'nif is required to register buyers and sellers'}
            return flask.jsonify(response)

        if 'home_addr' not in payload and 'shipping_addr' not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': 'address (home_addr or shipping_addr) is required to register buyers or sellers'}
            return flask.jsonify(response)

    try:
        if payload['type'] != 'buyers' and (payload['type'] == 'sellers' or payload['type'] == 'admins'):
            admin_check(f"to register {payload['type']}")

        values = [payload['user_id'], payload['username'], payload['password']]
        extra_values = [payload['user_id']]

        # TODO: test
        if payload['type'] == 'buyers':
            extra_values.append(payload['nif'])
            extra_values.append(payload['home_addr'])
        elif payload['type'] == 'sellers':
            extra_values.append(payload['nif'])
            extra_values.append(payload['shipping_addr'])

        statement = 'insert into users (user_id, username, password) values (%s, %s, %s);' \
                    f' insert into {payload["type"]} values (' + '%s, ' * (len(extra_values) - 1) + ' %s);'
        print(statement)
        values.extend(extra_values)

        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Registered user {payload["username"]}'}

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'GET /users - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /users - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/user/', methods=['PUT'])
def login_user():
    logger.info('PUT /users')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /users - payload: {payload}')

    if 'username' not in payload or 'password' not in payload:
        response = {'status': StatusCodes['bad_request'], 'results': 'username and password are required for login'}
        return flask.jsonify(response)

    statement = 'select user_id, username from users where username = %s and password = %s;'
    values = (payload['username'], payload['password'])

    try:
        cur.execute(statement, values)
        row = cur.fetchone()

        if row is not None:
            auth_token = jwt.encode({'user': row[0],
                                     'aud': app.config['SESSION_COOKIE_NAME'],
                                     'iat': datetime.datetime.utcnow(),
                                     'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=10)},
                                    app.config['SECRET_KEY'])

            try:
                jwt.decode(auth_token, app.config['SECRET_KEY'], audience=app.config['SESSION_COOKIE_NAME'],
                           algorithms=["HS256"])

            except jwt.exceptions.InvalidTokenError:
                raise TokenCreationError()

        else:
            raise InvalidAuthenticationException()

        response = {'status': StatusCodes['success'], 'token': auth_token}  # TODO: JWT authent
        # commit the transaction
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/questions/<product_id>', methods=['POST'])
@app.route('/dbproj/questions/<product_id>/<parents_question_id>', methods=['POST'])
def post_question(product_id=None, parents_question_id=None):
    logger.info('PUT /questions')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    if 'question' not in payload:
        response = {'status': StatusCodes['bad_request'],
                    'results': 'question must be provided for posting about a product'}
        return flask.jsonify(response)

    try:
        statement = 'select max(question_id), max(notification_id), max(products_version) from questions, notifications, products'

        cur.execute(statement)
        rows = cur.fetchone()

        products_version = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if len(rows) == 0:
            raise ProductNotFound(product_id)

        if rows[0] is None:
            question_id = 0
            notification_id = 0

        elif rows[0][0] is None and rows[0][1] is not None:
            # first question
            question_id = 0
            notification_id = rows[0][1] + 1
            products_version = rows[0][2].strftime("%Y-%m-%d %H:%M:%S")


        elif rows[0][1] is None:
            # first notification
            question_id = rows[0][0] + 1
            notification_id = 0
            products_version = rows[0][2].strftime("%Y-%m-%d %H:%M:%S")

        else:
            question_id = rows[0][0] + 1
            notification_id = rows[0][1] + 1
            products_version = rows[0][2].strftime("%Y-%m-%d %H:%M:%S")

        insert_question_values = [question_id, payload['question'], get_user_id(), notification_id, product_id,
                                  products_version]

        if parents_question_id is not None:
            # TODO: if parents_question_id is not None...
            parent_question_statement = 'select question_id, users_user_id from questions where question_id = %s;'
            parent_question_value = [parents_question_id]

            cur.execute(parent_question_statement, parent_question_value)
            parent_question_rows = cur.fetchone()

            if len(parent_question_rows) == 0:
                raise ParentQuestionNotFound(parents_question_id)

            insert_question_values.extend([parents_question_id, rows[1]])

        insert_question_statement = f'insert into questions values (%s, %s, %s, %s, %s, %s{", %s, %s" if parents_question_id is not None else ""});'

        print(insert_question_statement)

        cur.execute(insert_question_statement, insert_question_values)

        response = {'status': StatusCodes['success'], 'token': question_id}  # TODO: JWT authent

    except (TokenError, ProductNotFound,) as error:
        logger.error(f'GET /users - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
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
