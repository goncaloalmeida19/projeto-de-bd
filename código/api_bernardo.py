from datetime import datetime, timedelta
import flask
import logging
import jwt
import psycopg2

app = flask.Flask(__name__)
app.config['SECRET_KEY'] = 'my-32-character-ultra-secure-and-ultra-long-secret'
app.config['SESSION_COOKIE_NAME'] = 'our-db-project'

StatusCodes = {
    'success': 200,
    'bad_request': 400,
    'internal_error': 500
}

columns_names = {
    'products': ['product_id', 'version', 'name', 'price', 'stock', 'description', 'sellers_users_user_id'],
    'smartphones': ['screen_size', 'os', 'storage', 'color', 'products_product_id', 'products_version'],
    'televisions': ['screen_size', 'screen_type', 'resolution', 'smart', 'efficiency', 'products_product_id',
                    'products_version'],
    'computers': ['screen_size', 'cpu', 'gpu', 'storage', 'refresh_rate', 'products_product_id', 'products_version'],
    'campaigns': ['campaign_id', 'description', 'date_start', 'date_end', 'coupons', 'discount', 'admins_users_user_id']
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
        user='projuser',
        password='projuser',
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

@app.route('/products/<product_id>', methods=['GET'])
def get_product(product_id):
    logger.info('GET /products/<product_id>')
    user_token = flask.request.headers.get('Authorization').split()[1]
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

    buyer_id = flask.request.headers.get('Authorization').split(' ')[1]

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


@app.route('/dbproj/campaign/', methods=['POST'])
def add_campaign():
    logger.info('POST /campaign')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /campaign - payload: {payload}')

    # validate arguments
    for i in payload:
        if i not in columns_names['campaigns'][1:6]:
            response = {'status': StatusCodes['bad_request'],
                        'results': f'{i} is not a valid attribute'}
            return flask.jsonify(response)
    for i in range(1,6):
        if columns_names['campaigns'][i] not in payload:
            response = {'status': StatusCodes['bad_request'], 'results': f'{columns_names["campaigns"][i]} value not in payload'}
            return flask.jsonify(response)

    admin_id = 0

    verify_dates_statement = 'select exists(select 1 from campaigns where %s between date_start and date_end or %s between date_start and date_end)'
    verify_dates_values = (payload['date_start'], payload['date_end'])

    campaign_id_statement = 'select max(campaign_id)+1 from campaigns'

    # parameterized queries, good for security and performance
    campaign_statement = f'insert into campaigns ({",".join(list(payload))}, admins_users_user_id, campaign_id) ' \
                         f'values ({("%s," * len(payload))[:-1]},%s, %s)'
    print(campaign_statement)

    try:
        cur.execute(verify_dates_statement, verify_dates_values)
        if cur.fetchall()[0][0]:
            logger.error(f'POST /campaign - error: Another campaign is already running at that time')
            response = {'status': StatusCodes['bad_request'], 'errors': 'Another campaign is already running at that time'}
            return flask.jsonify(response)

        cur.execute(campaign_id_statement)
        campaign_id = cur.fetchone()[0]

        campaign_values = tuple(list(payload.values()) + [admin_id, campaign_id])
        print(campaign_values)

        cur.execute(campaign_statement, campaign_values)

        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Inserted campaign {campaign_id}'}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /campaign - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)

@app.route('/dbproj/subscribe/<campaign_id>', methods=['PUT'])
def subscribe_campaign(campaign_id):
    logger.info('PUT /subscribe/<campaign_id>')

    conn = db_connection()
    cur = conn.cursor()

    time_now = datetime.now()
    expiration_date = time_now + timedelta(days=30)

    user_id = 2

    time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
    expiration_date = expiration_date.strftime("%Y-%m-%d %H:%M:%S")

    #campaign_expired_statement = 'select exists(select 1 from campaigns )'

    # parameterized queries, good for security and performance
    subscribe_statement = 'update campaigns set coupons = coupons - 1 ' \
                          'where campaign_id = %s and %s between date_start and date_end and coupons > 0;'
    subscribe_values = (campaign_id, time_now)

    gen_coupon_statement = 'select coalesce(max(coupon_id) + 1, 1) from coupons'

    insert_coupon_statement = f'insert into coupons (coupon_id, used, discount_applied, expiration_date, campaigns_campaign_id, buyers_users_user_id) values (%s,%s,%s,%s,%s,%s);'


    try:
        cur.execute(subscribe_statement, subscribe_values)
        if cur.rowcount == 0:
            response = {'status': StatusCodes['bad_request'], 'results': 'That campaign is not available anymore'}
            return flask.jsonify(response)

        cur.execute(gen_coupon_statement)
        coupon_id = cur.fetchall()[0][0]

        insert_coupon_values = (coupon_id, 'false', 0, expiration_date, campaign_id, user_id)
        cur.execute(insert_coupon_statement, insert_coupon_values)

        response = {'status': StatusCodes['success'], 'results': {'coupon_id': coupon_id, 'expiration_date': expiration_date}}
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


@app.route('/product/<product_id>', methods=['PUT'])
def update_product(product_id):
    logger.info('PUT /product')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /product/<product_id> - payload: {payload}')

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    type_statement = 'select gettype(%s);'
    type_values = (product_id,)

    try:
        cur.execute(type_statement, type_values)
        product_type = cur.fetchall()[0][0]

        # get the data of the old version of the product
        old_items_statement = f'select * from products,{product_type} ' \
                              'where product_id = %s ' \
                              'and version =(select max(version) from products where product_id = %s) ' \
                              'and products_product_id = product_id and version = products_version'
        old_items_value = (product_id, product_id)
        cur.execute(old_items_statement, old_items_value)
        results = cur.fetchall()[0]

        # change the data, creating a new version
        new_data_products = tuple([payload[i] if i in list(payload.keys())
                                   else version if i == 'version'
        else results[columns_names['products'].index(i)]
                                   for i in columns_names['products']])
        new_data_product_type = tuple([payload[i] if i in list(payload.keys())
                                       else version if i == 'products_version'
        else results[columns_names[product_type].index(i) + len(columns_names['products'])]
                                       for i in columns_names[product_type]])

        # add the new version to the products table and corresponding product type table
        insert_products_statement = f'insert into products values({("%s," * len(columns_names["products"]))[:-1]});'
        insert_product_type_statement = f'insert into {product_type} values({("%s," * len(columns_names[product_type]))[:-1]});'
        cur.execute(insert_products_statement, new_data_products)
        cur.execute(insert_product_type_statement, new_data_product_type)
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Updated {",".join(list(payload.keys()))}'}
        """non_changed = list(set(columns_names[product_type] + columns_names['products']) - set(payload.keys()))

        old_items_statement = f"select {('%s,'*len(non_changed))[:-1]} from products, {product_type} where product_id = %s"
        old_items_value = tuple(non_changed + [product_id])

        print(old_items_statement, old_items_value)
        cur.execute(old_items_statement, old_items_value)
        results = cur.fetchall()

        response = {'status': StatusCodes['success'], 'results': results}"""

        # logger.debug('PUT /product/<product_id> - parse')
        # logger.debug(product_type)
        # content = {'ndep': int(row[0]), 'nome': row[1], 'localidade': row[2]}

        # response = {'status': StatusCodes['success'], 'results': content}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'PUT /product/<product_id> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

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

    required_product_input_info = products_columns_names[2:len(products_columns_names) - 1] + [
        'type']  # The type of the product is essential

    # logger.debug(f'POST /product - required_product_input_info: {required_product_input_info}')

    # Verification of the required parameters to add a product
    for i in required_product_input_info:
        if i not in payload:
            response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a product'}
            return flask.jsonify(response)

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    seller_id = flask.request.headers.get('Authorization').split(' ')[1]
    product_id_statement = 'select max(product_id) from products where sellers_users_user_id = %s'
    product_id_values = (seller_id,)

    # Statement and values about the info that will be insert to the "products" table
    product_statement = 'insert into products values (%s, %s, %s, %s, %s, %s, %s)'

    # Statement and values about the info that will be insert to the table that corresponds to the same type of product
    if payload['type'] == 'smartphones':
        required_smartphone_input_info = smartphones_columns_names[:len(smartphones_columns_names) - 2]
        for i in required_smartphone_input_info:
            if i not in payload:
                response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a smartphone'}
                return flask.jsonify(response)
        type_statement = 'insert into smartphones values (%s, %s, %s, %s, %s, %s)'
        type_values = tuple(payload[i] for i in required_smartphone_input_info)
    elif payload['type'] == 'televisions':
        required_television_input_info = televisions_columns_names[:len(televisions_columns_names) - 2]
        for i in required_television_input_info:
            if i not in payload:
                response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a smartphone'}
                return flask.jsonify(response)
        type_statement = 'insert into televisions values (%s, %s, %s, %s, %s, %s, %s)'
        type_values = tuple(payload[i] for i in required_television_input_info)
    elif payload['type'] == 'computers':
        required_computer_input_info = computers_columns_names[:len(computers_columns_names) - 2]
        for i in required_computer_input_info:
            if i not in payload:
                response = {'status': StatusCodes['api_error'], 'results': f'{i} is required to add a smartphone'}
                return flask.jsonify(response)
        type_statement = 'insert into computers values (%s, %s, %s, %s, %s, %s, %s)'
        type_values = tuple(payload[i] for i in required_computer_input_info)
    else:
        response = {'status': StatusCodes['api_error'], 'results': 'valid type is required to add a product'}
        return flask.jsonify(response)

    try:
        # Get new product_id
        cur.execute(product_id_statement, product_id_values)
        rows = cur.fetchall()
        product_id = rows[0][0] + 1

        product_values = (
            product_id, version, payload['name'], payload['price'], payload['stock'], payload['description'], seller_id)
        type_values += (product_id, version)

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
    buyers_id = flask.request.headers.get('Authorization').split(' ')[1]

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
        admin_check()

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
