from datetime import datetime
import flask
import logging
import jwt
import psycopg2

app = flask.Flask(__name__)
app.config['SECRET_KEY'] = 'my-32-character-ultra-secure-and-ultra-long-secret'
app.config['SESSION_COOKIE_NAME'] = 'our-db-project'

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}

columns_names = {
    "ratings": ["comment", "rating", "orders_id", "products_product_id", "products_version", "buyers_users_user_id"],
    "products": ['product_id', 'version', 'name', 'price', 'stock', 'description', 'sellers_users_user_id'],
    "smartphones": ['screen_size', 'os', 'storage', 'color', 'products_product_id', 'products_version'],
    "televisions": ['screen_size', 'screen_type', 'resolution', 'smart', 'efficiency', 'products_product_id',
                    'products_version'],
    "computers": ['screen_size', 'cpu', 'gpu', 'storage', 'refresh_rate', 'products_product_id', 'products_version'],
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


class NoDataFound(Exception):
    pass


##########################################################
# AUXILIARY FUNCTIONS
##########################################################

def admin_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()

    try:
        user_token = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                                audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])
        print(user_token)
    except jwt.exceptions.InvalidTokenError as e:
        raise TokenError()

    admin_validation = 'select users_user_id ' \
                       'from admins ' \
                       'where users_user_id = %s'

    cur.execute(admin_validation, [user_token['user']])

    if cur.fetchone() is None:
        raise InsufficientPrivilegesException(fail_msg)


##########################################################
# DATABASE ACCESS
##########################################################

def db_connection():
    db = psycopg2.connect(
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432',
        database='dbproj'
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

@app.route('/dbproj/products/<product_id>', methods=['GET'])
def get_product(product_id):
    logger.info('GET /products/<product_id>')
    conn = db_connection()
    cur = conn.cursor()

    try:
        # Get info about the product that have the product_id correspondent to the one given
        statement = 'select name, stock, description, (select avg(rating) :: float from ratings), comment, price, version ' \
                    'from products, ratings ' \
                    'group by products_product_id, name, stock, description, comment, price, version, product_id, ratings.products_version ' \
                    'having products_product_id = %s and product_id = %s and products.version = ratings.products_version'
        values = (product_id, product_id)

        cur.execute(statement, values)
        rows = cur.fetchall()
        if len(rows) != 0:
            logger.debug(rows)

            prices = [f"{i[6]} - {i[5]}" for i in rows]
            comments = [i[4] for i in rows]
            content = {'name': rows[0][0], 'stock': rows[0][1], 'description': rows[0][2], 'prices': prices,
                       'rating': rows[0][3], 'comments': comments}

            # Response of the status of obtaining a product and the information obtained
            response = {'status': StatusCodes['success'], 'results': content}
        else:
            raise NoDataFound
    except NoDataFound:
        error = f"No product found with id: {product_id}"
        logger.error(f'GET /product/<product_id> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
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

@app.route('/dbproj/rating/<product_id>', methods=['POST'])
def give_rating_feedback(product_id):
    logger.info('POST /rating/<product_id>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /rating/<product_id> - payload: {payload}')

    # Verification of the required parameters to do a rating to a product
    for i in columns_names["ratings"][:2]:
        if i not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to rate a product'}
            return flask.jsonify(response)

    buyer_id = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                          audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])['user']

    try:
        # Get info about the product that will be rated (the one already bought) and insert the rating info in the "ratings" table
        statement = 'do $$ ' \
                    'declare ' \
                    'order_id int; ' \
                    'version timestamp; ' \
                    'begin ' \
                    '   select orders.id, product_quantities.products_version ' \
                    '   from product_quantities, orders ' \
                    '   where product_quantities.products_product_id = %s ' \
                    '   and product_quantities.orders_id = orders.id ' \
                    '   and orders.buyers_users_user_id = %s ' \
                    '   into order_id, version; ' \
                    '   insert into ratings values (%s, %s, order_id, %s, version, %s); ' \
                    'end; ' \
                    '$$; '
        values = (product_id, buyer_id) + tuple(payload[i] for i in columns_names["ratings"][:2]) + (
            product_id, buyer_id)

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

@app.route('/dbproj/product', methods=['POST'])
def add_product():
    logger.info('POST /product')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # The type of the product is essential
    required_input_info = dict(
        (item, value[:-2] + ['type']) if item != "products" and item != "ratings" else (item, value[2: -1]) for
        item, value in columns_names.items())

    # logger.debug(f'POST /product - required_product_input_info: {required_product_input_info}')

    # Verification of the required parameters to add a product
    for i in required_input_info["products"]:
        if i not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a product'}
            return flask.jsonify(response)

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    seller_id = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                           audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])['user']
    product_type = payload['type']

    try:
        # Get new product_id
        product_id_statement = 'select max(product_id) from products where sellers_users_user_id = %s'
        product_id_values = (seller_id,)
        cur.execute(product_id_statement, product_id_values)
        rows = cur.fetchall()
        product_id = rows[0][0] + 1

        final_statement = 'do $$ ' \
                          'begin ' \
                          'insert into products values (%s, %s, %s, %s, %s, %s, %s); ' \
                          ''
        final_values = (
            str(product_id), version, payload['name'], str(payload['price']), str(payload['stock']),
            payload['description'],
            str(seller_id))

        # Statement and values about the info that will be insert to the table that corresponds to the same type of product
        if product_type in list(columns_names)[2:]:
            for j in required_input_info[product_type]:
                if j not in payload:
                    response = {'status': StatusCodes['api_error'],
                                'results': f'{j} is required to add a {product_type[:-1]}'}
                    return flask.jsonify(response)

            final_statement += f'insert into {product_type} values (' + ('%s, ' * len(
                columns_names[product_type]))[:-2] + '); ' \
                                                     'end;' \
                                                     '$$;'
            final_values += tuple(str(payload[i]) for i in required_input_info[product_type][:-1]) + tuple(
                [str(product_id), version])
        else:
            response = {'status': StatusCodes['api_error'], 'results': 'valid type is required to add a product'}
            return flask.jsonify(response)

        # logger.debug(final_statement)
        # logger.debug(final_values)

        # Insert new product info in table that corresponds to the same type of product
        cur.execute(final_statement, final_values)

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


##
# Buy products, an order, based on a JSON payload
##
# To use it, you need to use postman:
##
# http://localhost:8080/order
##

@app.route('/dbproj/order', methods=['POST'])
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

    product_version_statement = 'select max(version), price from products where product_id = %s group by price '
    campaign_statement = 'select campaigns_campaign_id from coupons where coupon_id = %s '
    order_id_statement = 'select max(id) from orders '

    product_quantities_statement = 'insert into product_quantities values (%s, %s, %s, %s)'
    order_statement = 'insert into orders (id, order_date, buyers_users_user_id) values (%s, %s, %s)'
    order_with_campaign_statement = 'insert into orders (id, order_date, buyers_users_user_id, coupons_coupon_id, coupons_campaigns_campaign_id) values (%s, %s, %s, %s, %s)'

    order_price_update_statement = 'update orders set price_total = %s where id = %s'

    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_price = 0.0  # Without coupon
    buyers_id = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                           audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])['user']

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

    if 'username' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'username is required for user registry'}
        return flask.jsonify(response)

    if 'password' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'password is required for user registry'}
        return flask.jsonify(response)

    if 'type' not in payload or payload['type'] not in ['buyers', 'sellers', 'admins']:
        response = {'status': StatusCodes['api_error'], 'results': 'user type is required for user registry: buyers, '
                                                                   'sellers or admins'}
        return flask.jsonify(response)

    statement = 'insert into users (user_id, username, password) values (%s, %s, %s)'
    values = (payload['user_id'], payload['username'], payload['password'])

    try:
        if payload['type'] != 'buyers' and (payload['type'] == 'sellers' or payload['type'] == 'admins'):
            admin_check(f"to register {payload['type']}")

        cur.execute(statement, values)
        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Registered user {payload["username"]}'}

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

    statement = 'select user_id, username from users where username = %s and password = %s'
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
