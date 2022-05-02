from datetime import datetime
import flask
import logging
import psycopg2

app = flask.Flask(__name__)

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}

products_types = ["smartphones", "televisions", "computers"]
products_columns_names = ['product_id', 'version', 'name', 'price', 'stock', 'description', 'sellers_users_user_id']
smartphones_columns_names = ['screen_size', 'os', 'storage', 'color', 'products_product_id', 'products_version']
televisions_columns_names = ['screen_size', 'screen_type', 'resolution', 'smart', 'efficiency', 'products_product_id',
                             'products_version']
computers_columns_names = ['screen_size', 'cpu', 'gpu', 'storage', 'refresh_rate', 'products_product_id',
                           'products_version']


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

    conn = db_connection()
    cur = conn.cursor()

    try:
        # Get info about the product that have the product_id correspondent to the one given
        statement = 'select name, stock, description, (select avg(classification) :: float from ratings), comment, price, version ' \
                    'from products, ratings ' \
                    'group by products_product_id, name, stock, description, comment, price, version, product_id, ratings.products_version ' \
                    'having products_product_id = %s and product_id = %s and products.version = ratings.products_version'
        values = (product_id, product_id)
        cur.execute(statement, values)
        rows = cur.fetchall()
        prices = [f"{i[6]} - {i[5]}" for i in rows]
        comments = [i[4] for i in rows]
        content = {'name': rows[0][0], 'stock': rows[0][1], 'description': rows[0][2], 'prices': prices,
                   'rating': rows[0][3], 'comments': comments}

        # Response of the status of obtaining a product and the information obtained
        response = {'status': StatusCodes['success'], 'results': content}

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

    # Verification of the required parameters to do a rating to a product
    if 'rating' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'rating is required to rate a product'}
        return flask.jsonify(response)
    elif 'comment' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'comment is required to rate a product'}
        return flask.jsonify(response)

    buyer_id = "2"

    try:
        # Get info about the product that will be rated (the one already bought)
        statement = 'select orders.id, product_quantities.products_version ' \
                    'from product_quantities, orders ' \
                    'where product_quantities.products_product_id = %s ' \
                    'and product_quantities.orders_id = orders.id ' \
                    'and orders.buyers_users_user_id = %s'
        values = (product_id, buyer_id,)
        cur.execute(statement, values)
        rows = cur.fetchall()
        order_id = rows[0][0]
        version = rows[0][1].strftime("%Y-%m-%d %H:%M:%S")

        # Insert the rating info in the "ratings" table
        statement = 'insert into ratings values (%s, %s, %s, %s, %s, %s)'
        values = (payload['comment'], payload['rating'], order_id, product_id, version, buyer_id)
        cur.execute(statement, values)

        # Response of the rating status
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

    required_product_input_info = products_columns_names[2:len(products_columns_names) - 1].copy()
    required_product_input_info.append('type')  # The type of the product is essential

    # logger.debug(f'POST /product - required_product_input_info: {required_product_input_info}')

    # Verification of the required parameters to add a product
    for i in required_product_input_info:
        if i not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a product'}
            return flask.jsonify(response)

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    product_id = "69424"
    seller_id = "1"

    # Statement and values about the info that will be insert to the "products" table
    product_statement = 'insert into products values (%s, %s, %s, %s, %s, %s, %s)'
    product_values = (product_id, version, payload['name'], payload['price'], payload['stock'], payload['description'],
                      seller_id)

    # Statement and values about the info that will be insert to the table that corresponds to the same type of product
    if payload['type'] == 'smartphones':
        required_smartphone_input_info = smartphones_columns_names[:len(smartphones_columns_names) - 2]
        for i in required_smartphone_input_info:
            if i not in payload:
                response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a smartphone'}
                return flask.jsonify(response)
        type_statement = 'insert into smartphones values (%s, %s, %s, %s, %s, %s)'
        type_values = (
            payload['screen_size'], payload['os'], payload['storage'], payload['color'], product_id, version,)
    elif payload['type'] == 'televisions':
        required_television_input_info = televisions_columns_names[:len(televisions_columns_names) - 2]

        for i in required_television_input_info:
            if i not in payload:
                response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a smartphone'}
                return flask.jsonify(response)
        type_statement = 'insert into televisions values (%s, %s, %s, %s, %s, %s, %s)'
        type_values = (
            payload['screen_size'], payload['screen_type'], payload['resolution'], payload['smart'],
            payload['efficiency'],
            product_id, version,)
    elif payload['type'] == 'computers':
        required_computer_input_info = computers_columns_names[:len(computers_columns_names) - 2]
        for i in required_computer_input_info:
            if i not in payload:
                response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a smartphone'}
                return flask.jsonify(response)
        type_statement = 'insert into computers values (%s, %s, %s, %s, %s, %s, %s)'
        type_values = (
            payload['screen_size'], payload['cpu'], payload['gpu'], payload['storage'], payload['refresh_rate'],
            product_id,
            version,)
    else:
        response = {'status': StatusCodes['api_error'], 'results': 'valid type is required to add a product'}
        return flask.jsonify(response)

    try:
        # Insert new product info in "products" table
        cur.execute(product_statement, product_values)

        # Insert new product info in table that corresponds to the same type of product
        cur.execute(type_statement, type_values)

        # Response of the adding the product status
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


def get_product_type(product_id):
    logger.info('GET /products/<product_id>')
    # logger.debug(f'product_id: {product_id}')

    conn = db_connection()
    cur = conn.cursor()

    # parameterized queries, good for security and performance
    statement = 'do $$ ' \
                'declare ' \
                'begin ' \
                'perform products_product_id from smartphones where products_product_id = %s; ' \
                'return 0;' \
                'exception ' \
                '   when no_data_found then ' \
                '       perform products_product_id from televisions where products_product_id = %s; ' \
                '       return 1;' \
                '       exception ' \
                '           when no_data_found then ' \
                '               perform products_product_id from computers where products_product_id = %s; ' \
                '               return 2;' \
                '               exception ' \
                '                   when no_data_found then ' \
                '                       return 3;' \
                'end; ' \
                '$$;'
    values = (product_id, product_id, product_id,)

    try:
        cur.execute(statement, values)
        rows = cur.fetchall()
        product_type = rows[0][0]
        if int(product_type) == len(products_types):
            return None
        return products_types[int(product_type)]

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Get the product to update - error: {error}')

    finally:
        if conn is not None:
            conn.close()


def get_product_to_update(product_id):
    return


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

    product = get_product_to_update(product_id)

    statements = []
    values_table = []
    if 'type' not in payload:
        for i in payload.keys:
            statements.append('update products set i = %s where product_id = %s')
            values_table.append((payload[i], product_id,))
    else:
        if payload['type'] == 'smartphones':
            for i in payload.keys:
                if i in smartphones_columns_names:
                    statements.append('update smartphones set i = %s where products_product_id = %s')
                    values_table.append((payload[i], product_id,))
                else:
                    statements.append('update products set i = %s where product_id = %s')
                    values_table.append((payload[i], product_id,))
        elif payload['type'] == 'computers':
            for i in payload.keys:
                if i in computers_columns_names:
                    statements.append('update computers set i = %s where products_product_id = %s')
                    values_table.append((payload[i], product_id,))
                else:
                    statements.append('update products set i = %s where product_id = %s')
                    values_table.append((payload[i], product_id,))
        elif payload['type'] == 'televisions':
            for i in payload.keys:
                if i in televisions_columns_names:
                    statements.append('update televisions set i = %s where products_product_id = %s')
                    values_table.append((payload[i], product_id,))
                else:
                    statements.append('update products set i = %s where product_id = %s')
                    values_table.append((payload[i], product_id,))
        else:
            response = {'status': StatusCodes['api_error'], 'results': 'valid type is required to update a product'}
            return flask.jsonify(response)

    try:
        for i in range(len(statements)):
            cur.execute(statements[i], values_table[i])
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


##
# Buy products, an order, based on a JSON payload
##
# To use it, you need to use postman:
##
# http://localhost:8080/order
##

@app.route('/order', methods=['POST'])
def buy_products():
    logger.info('POST /order')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'POST /order - payload: {payload}')

    coupon_id = -1

    if 'cart' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'cart is required to buy products'}
        return flask.jsonify(response)
    if 'coupon' in payload:
        coupon_id = payload['coupon_id']

    product_version_statement = 'select max(version), price from products where product_id = %s group by price'
    product_quantities_statement = 'insert into product_quantities values (%s, %s, %s, %s)'
    campaign_statement = 'select campaigns_campaign_id from coupons where coupon_id = %s'
    order_id_statement = 'select max(id) from orders'
    order_statement = 'insert into orders (id, order_date, buyers_users_user_id) values (%s, %s, %s)'
    order_with_campaign_statement = 'insert into orders (id, order_date, buyers_users_user_id, coupons_coupon_id, coupons_campaigns_campaign_id) values (%s, %s, %s, %s, %s)'
    order_price_update_statement = 'update orders set price_total = %s where id = %s'

    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_price = 0.0  # Without coupon
    buyers_id = "2"

    try:
        cur.execute(order_id_statement, )
        order_id = cur.fetchall()[0][0] + 1
        # logger.debug(f'{order_id}')

        if coupon_id != -1:
            campaign_values = (coupon_id,)
            cur.execute(campaign_statement, campaign_values)
            campaign_id = cur.fetchall()[0][0]
            order_with_campaign_values = (order_id, order_date, buyers_id, coupon_id, campaign_id)
            cur.execute(order_with_campaign_statement, order_with_campaign_values)
        else:
            order_values = (order_id, order_date, buyers_id)
            cur.execute(order_statement, order_values)

        for i in payload['cart']:
            # logger.debug(f'{i}')

            product_version_values = (i['product_id'],)
            cur.execute(product_version_statement, product_version_values)
            rows = cur.fetchall()
            version = rows[0][0].strftime("%Y-%m-%d %H:%M:%S")
            # logger.debug(f'{rows[0][1]}')
            total_price += rows[0][1]
            # logger.debug(f'{version}')

            product_quantities_values = (i['quantity'], order_id, i['product_id'], version)
            cur.execute(product_quantities_statement, product_quantities_values)

        order_price_update_values = (total_price, order_id,)
        cur.execute(order_price_update_statement, order_price_update_values)

        response = {'status': StatusCodes['success'], 'results': f'{order_id}'}
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
