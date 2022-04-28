import flask
import logging
import psycopg2
import time

app = flask.Flask(__name__)

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}


class AuthenticationException(Exception):
    def __init__(self, message='User must be administrator'):
        super(AuthenticationException, self).__init__(message)


##########################################################
# DATABASE ACCESS
##########################################################

def db_connection():
    db = psycopg2.connect(
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432',
        database='projeto'
    )

    return db


##########################################################
# ENDPOINTS
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
# Obtain product with product_id <product_id>
##
# To use it, access:
##
# http://localhost:8080/products/7390626
##

@app.route('/products/<product_id>', methods=['GET'])
def get_product(product_id):
    logger.info('GET /products/<product_id>')

    # logger.debug(f'product_id: {product_id}')

    conn = db_connection()
    cur = conn.cursor()

    # parameterized queries, good for security and performance
    statement = 'select name, stock, description, (select avg(classification) :: float from ratings), comment, price, version ' \
                'from products, ratings ' \
                'group by products_product_id, name, stock, description, comment, price, version, product_id, ratings.products_version ' \
                'having products_product_id = %s and product_id = %s and products.version = ratings.products_version'
    values = (product_id, product_id)

    try:
        cur.execute(statement, values)
        rows = cur.fetchall()

        row = rows[0]
        # logger.debug('GET /departments/<product_id> - parse')
        # logger.debug(row)

        prices = [f"{i[6]} - {i[5]}" for i in rows]
        comments = [i[4] for i in rows]
        content = {'name': row[0], 'stock': row[1], 'description': row[2], 'prices': prices, 'rating': row[3],
                   'comments': comments}

        response = {'status': StatusCodes['success'], 'results': content}
        # "errors": errors( if any occurs)},

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /product/<product_id> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Give rating/feedback
##
## To use it, you need to use postman or curl:
##
## http://localhost:8080/rating/7390626
##

@app.route('/rating/<product_id>', methods=['PUT'])
def give_rating_feedback(product_id):
    logger.info('POST /rating/<product_id>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /rating/<product_id> - payload: {payload}')

    # do not forget to validate every argument, e.g.,:
    if 'rating' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'rating is required to update'}
        return flask.jsonify(response)
    elif 'comment' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'comment is required to update'}
        return flask.jsonify(response)

    first_statement = 'select orders.id, product_quantities.products_version, orders.buyers_users_user_id ' \
                      'from product_quantities, orders ' \
                      'where product_quantities.products_product_id = %s ' \
                      'and product_quantities.orders_id = orders.id'
    first_values = (product_id,)
    order_id = buyer_id = 0
    version = ""
    second_statement = 'insert into ratings values (%s, %d, %d, %d, %s, %d)'
    second_values = (payload['comment'], int(payload['rating']), order_id, product_id, version, buyer_id)

    try:
        cur.execute(first_statement, first_values)
        rows = cur.fetchall()
        order_id = int(rows[0][0])
        version = rows[0][1]
        buyer_id = int(rows[0][2])
        res = cur.execute(second_statement, second_values)
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
            raise AuthenticationException()

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
            raise AuthenticationException()

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

    statement = 'SELECT user_id FROM users WHERE username = %s AND password = %s'
    values = (payload['username'], payload['password'])

    try:
        cur.execute(statement, values)
        row = cur.fetchone()

        response = {'status': StatusCodes['success'], 'token': row[0]}  # TODO: JWT authent

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
