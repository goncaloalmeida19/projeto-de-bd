from datetime import datetime
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

products_columns_names = ['product_id', 'version', 'name', 'price', 'stock', 'description', 'sellers_users_user_id']
smartphones_columns_names = ['screen_size', 'os', 'storage', 'color', 'products_product_id', 'products_version']
televisions_columns_names = ['screen_size', 'screen_type', 'resolution', 'smart', 'efficiency', 'products_product_id', 'products_version']
computers_column_names = ['screen_size', 'cpu', 'gpu', 'storage', 'refresh_rate', 'products_product_id', 'products_version']


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
# Give rating/feedback based on a JSON payload
##
# To use it, you need to use postman:
##
# http://localhost:8080/rating/7390626
##

@app.route('/rating/<product_id>', methods=['POST'])
def give_rating_feedback(product_id):
    logger.info('POST /rating/<product_id>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /rating/<product_id> - payload: {payload}')

    # do not forget to validate every argument, e.g.,:
    if 'rating' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'rating is required to rate a product'}
        return flask.jsonify(response)
    elif 'comment' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'comment is required to rate a product'}
        return flask.jsonify(response)

    first_statement = 'select orders.id, product_quantities.products_version, orders.buyers_users_user_id ' \
                      'from product_quantities, orders ' \
                      'where product_quantities.products_product_id = %s ' \
                      'and product_quantities.orders_id = orders.id'
    first_values = (product_id,)
    second_statement = 'insert into ratings values (%s, %s, %s, %s, %s, %s)'

    try:
        cur.execute(first_statement, first_values)
        rows = cur.fetchall()
        order_id = rows[0][0]
        version = rows[0][1].strftime("%Y-%m-%d %H:%M:%S")
        buyer_id = rows[0][2]
        second_values = (payload['comment'], payload['rating'], order_id, product_id, version, buyer_id)
        # logger.debug(f'{second_values}')
        res = cur.execute(second_statement, second_values)
        # logger.debug(f'POST /rating/<product_id> - res: {res}')
        response = {'status': StatusCodes['success']}

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


##
# Add product based on a JSON payload
##
# To use it, you need to use postman:
##
# http://localhost:8080/product
##

@app.route('/product', methods=['POST'])
def add_product():
    logger.info('POST /product')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /product - payload: {payload}')

    if 'description' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'description is required to add a product'}
        return flask.jsonify(response)
    elif 'type' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'type is required to add a product'}
        return flask.jsonify(response)
    elif 'stock' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'stock is required to add a product'}
        return flask.jsonify(response)
    elif 'price' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'price is required to add a product'}
        return flask.jsonify(response)
    elif 'name' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'name is required to add a product'}
        return flask.jsonify(response)

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    product_id = "69420"
    seller_id = "1"

    product_statement = 'insert into products values (%s, %s, %s, %s, %s, %s, %s)'
    product_values = (product_id, version, payload['name'], payload['price'], payload['stock'], payload['description'],
                      seller_id)

    if payload['type'] == 'smartphones':
        if 'screen_size' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'screen_size is required to add a smartphone'}
            return flask.jsonify(response)
        elif 'os' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'os is required to add a smartphone'}
            return flask.jsonify(response)
        elif 'storage' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'storage is required to add a smartphone'}
            return flask.jsonify(response)
        elif 'color' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'color is required to add a smartphone'}
            return flask.jsonify(response)
        type_statement = 'insert into smartphones values (%s, %s, %s, %s, %s, %s)'
        type_values = (payload['screen_size'], payload['os'], payload['storage'], payload['color'], product_id,
                       version,)
    elif payload['type'] == 'televisions':
        if 'screen_size' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'screen_size is required to add a television'}
            return flask.jsonify(response)
        elif 'screen_type' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'screen_type is required to add a television'}
            return flask.jsonify(response)
        elif 'resolution' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'resolution is required to add a television'}
            return flask.jsonify(response)
        elif 'smart' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'smart is required to add a television'}
            return flask.jsonify(response)
        elif 'efficiency' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'efficiency is required to add a television'}
            return flask.jsonify(response)
        type_statement = 'insert into televisions values (%s, %s, %s, %s, %s, %s, %s)'
        type_values = (payload['screen_size'], payload['screen_type'], payload['resolution'], payload['smart'],
                       payload['efficiency'], product_id, version,)
    elif payload['type'] == 'computers':
        if 'screen_size' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'screen_size is required to add computer'}
            return flask.jsonify(response)
        elif 'cpu' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'cpu is required to add a computer'}
            return flask.jsonify(response)
        elif 'gpu' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'gpu is required to add a computer'}
            return flask.jsonify(response)
        elif 'storage' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'storage is required to add a computer'}
            return flask.jsonify(response)
        elif 'refresh_rate' not in payload:
            response = {'status': StatusCodes['api_error'], 'results': 'refresh_rate is required to add a computer'}
            return flask.jsonify(response)
        type_statement = 'insert into computers values (%s, %s, %s, %s, %s, %s, %s)'
        type_values = (payload['screen_size'], payload['cpu'], payload['gpu'], payload['storage'],
                       payload['refresh_rate'], product_id, version,)
    else:
        response = {'status': StatusCodes['api_error'], 'results': 'valid type is required to add a product'}
        return flask.jsonify(response)

    try:
        cur.execute(product_statement, product_values)
        cur.execute(type_statement, type_values)
        response = {'status': StatusCodes['success'], 'results': f'{product_id}'}
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


##
# Update a product based on a JSON payload
##
# To use it, you need to use postman:
##
# http://localhost:8080/product/69420
##
@app.route('/product/<product_id>', methods=['PUT'])
def update_product(product_id):
    logger.info('PUT /product/<product_id>')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /product/<product_id> - payload: {payload}')

    product_statements = []
    product_values_table = []
    if 'type' not in payload:
        for i in payload.keys:
            product_statements.append('update products set i = %s where product_id = %s')
            product_values_table.append((payload[i], product_id,))
    else:
        type_statements = []
        type_values_table = []
        if payload['type'] == 'smartphones':
            for i in payload.keys:
                if i in smartphones_columns_names:
                    product_statements.append('update smartphones set i = %s where products_product_id = %s')
                    product_values_table.append((payload[i], product_id,))
                else:
                    product_statements.append('update products set i = %s where product_id = %s')
                    product_values_table.append((payload[i], product_id,))
        elif payload['type'] == 'computers':
            for i in payload.keys:
                if i in computers_column_names:
                    product_statements.append('update computers set i = %s where products_product_id = %s')
                    product_values_table.append((payload[i], product_id,))
                else:
                    product_statements.append('update products set i = %s where product_id = %s')
                    product_values_table.append((payload[i], product_id,))
        elif payload['type'] == 'televisions':
            for i in payload.keys:
                if i in televisions_columns_names:
                    product_statements.append('update televisions set i = %s where products_product_id = %s')
                    product_values_table.append((payload[i], product_id,))
                else:
                    product_statements.append('update products set i = %s where product_id = %s')
                    product_values_table.append((payload[i], product_id,))
        else:
            response = {'status': StatusCodes['api_error'], 'results': 'valid type is required to update a product'}
            return flask.jsonify(response)

    try:
        res = cur.execute(product_statement, product_values)
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
