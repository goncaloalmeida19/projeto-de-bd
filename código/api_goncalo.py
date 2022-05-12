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
    "ratings": ["comment", "rating", "orders_id", "products_product_id", "products_version", "buyers_users_user_id"],
    "products": ['product_id', 'version', 'name', 'price', 'stock', 'description', 'sellers_users_user_id'],
    "smartphones": ['screen_size', 'os', 'storage', 'color', 'products_product_id', 'products_version'],
    "televisions": ['screen_size', 'screen_type', 'resolution', 'smart', 'efficiency', 'products_product_id',
                    'products_version'],
    "computers": ['screen_size', 'cpu', 'gpu', 'storage', 'refresh_rate', 'products_product_id', 'products_version'],
    "campaigns": ['campaign_id', 'description', 'date_start', 'date_end', 'coupons', 'discount', 'admins_users_user_id'],
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


class ProductWithoutStockAvailable(Exception):
    def __init__(self, p_id, p_quantity, p_stock,
                 message1="The seller hasn't the required quantity in stock of the product with id '"):
        super(ProductWithoutStockAvailable, self).__init__(
            message1 + p_id + "': Quantity: '" + p_quantity + "' \\ Stock: '" + p_stock + "'")


class CouponNotFound(Exception):
    def __init__(self, c_id, message='No coupon found with id: '):
        super(CouponNotFound, self).__init__(message + c_id)


class CouponExpired(Exception):
    def __init__(self, c_id, e_date, t_date, message1="The coupon with id '", message2="' has expired in '"):
        super(CouponExpired, self).__init__(message1 + c_id + message2 + e_date + "' and today is '" + t_date + "'")


class AlreadyRated(Exception):
    def __init__(self, p_id, p_version, o_id, p_r, p_c, message1="Product with id '", message2="' and version '",
                 message3="' from order '", message4="' already been rated: "):
        super(AlreadyRated, self).__init__(
            message1 + p_id + message2 + p_version + message3 + o_id + message4 + "Rating: '" + p_r + ' \\ Comment: ' + p_c + "'")


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

    admin_validation = 'select users_user_id from admins where users_user_id = %s'

    cur.execute(admin_validation, [user_token['user']])

    if cur.fetchone() is None:
        raise InsufficientPrivilegesException(fail_msg)


##########################################################
# DATABASE ACCESS
##########################################################

def db_connection():
    db = psycopg2.connect(
        user='postgres',
        password='postgres',  # TODO: *******
        host='127.0.0.1',
        port='5432',
        database='dbproj'
    )

    return db


##########################################################
# ENDPOINTS
##########################################################


@app.route('/dbproj/')
def landing_page():
    return """

    Welcome to EANOS!!!  <br/>
    <br/>
    Check the sources for instructions on how to use the endpoints!<br/>
    <br/>
    Best BD students of 2022<br/>
    <br/>
    """


##
# Obtain product with product_id <product_id>
##
# To use it, access:
##
# http://localhost:8080/dbproj/products/7390626
##

@app.route('/dbproj/products/<product_id>', methods=['GET'])
def get_product(product_id):
    logger.info('GET /products/<product_id>')
    conn = db_connection()
    cur = conn.cursor()

    try:
        # Get info about the product that have the product_id correspondent to the one given
        statement = 'select * from products, ratings where product_id = 69420'
        values = (product_id, product_id)
        cur.execute(statement, values)
        rows = cur.fetchall()
        logger.debug(rows)

        if len(rows) == 0:
            raise ProductNotFound(product_id)

        prices = [f"{i[6]} - {i[5]}" for i in rows]  # Format: product_price_version - product_price_associated_to_the_version
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
# http://localhost:8080/dbproj/rating/69420
##

@app.route('/dbproj/rating/<product_id>', methods=['POST'])
def give_rating_feedback(product_id):
    logger.info('POST /rating/<product_id>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'POST /rating/<product_id> - payload: {payload}')

    # If there are more fields than the necessary ones in the request, consider it a bad one
    if len(payload) > 2:
        response = {'status': StatusCodes['bad_request'], 'results': 'Invalid number of fields in the payload'}
        return flask.jsonify(response)

    # Verification of the required fields to do a rating to a product
    for i in columns_names["ratings"][:2]:
        if i not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': f'{i.capitalize()} is required to rate a product'}
            return flask.jsonify(response)

    # A rating needs to be between 1 and 5 if not consider the request a bad one
    if not 1 <= payload['rating'] <= 5:
        response = {'status': StatusCodes['bad_request'], 'results': f'A valid rating is required to add a product'}
        return flask.jsonify(response)

    # Get the buyer id
    buyer_id = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                          audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])['user']

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

        if len(rows) == 0:
            raise ProductNotFound(product_id)

        # Get the most recent order id related to the most recent version of the product with id <product_id>
        order_id = rows[len(rows) - 1][0]
        version = rows[len(rows) - 1][1].strftime("%Y-%m-%d %H:%M:%S")

        # Verify if the product have already been rated
        statement = 'select orders_id, rating, comment ' \
                    'from ratings ' \
                    'where orders_id = %s ' \
                    'and products_product_id = %s'
        values = (order_id, product_id,)
        cur.execute(statement, values)
        rows = cur.fetchall()

        if len(rows) != 0:
            raise AlreadyRated(product_id, version, order_id, rows[0][1], rows[0][2])

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
# http://localhost:8080/dbproj/product
##

@app.route('/dbproj/product', methods=['POST'])
def add_product():
    logger.info('POST /product')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # The type of the product is essential
    required_input_info = dict(
        (item, value[:-2] + ['type']) if item not in ["products", "ratings", "campaigns"] else (item, value[2: -1]) for
        item, value in columns_names.items())

    # logger.debug(f'POST /product - required_product_input_info: {required_product_input_info}')

    # Verification of the required fields to add a product
    for i in required_input_info["products"]:
        if i not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': f'{i.capitalize()} is required to add a product'}
            return flask.jsonify(response)

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    product_type = payload['type']

    # Get the seller id
    seller_id = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                           audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])['user']

    try:
        # Get new product_id
        product_id_statement = 'select max(product_id) from products where sellers_users_user_id = %s'
        product_id_values = (seller_id,)
        cur.execute(product_id_statement, product_id_values)
        rows = cur.fetchall()
        product_id = rows[0][0] + 1 if rows[0][0] is not None else 1

        final_statement = 'do $$ ' \
                          'begin ' \
                          'insert into products values (%s, %s, %s, %s, %s, %s, %s); ' \
                          ''
        final_values = (
            str(product_id), version, payload['name'], str(payload['price']), str(payload['stock']),
            payload['description'],
            str(seller_id))

        # Statement and values about the info that will be insert to the table that corresponds to the same type of product
        if product_type in list(columns_names)[2:-1]:
            for j in required_input_info[product_type]:
                if j not in payload:
                    response = {'status': StatusCodes['bad_request'],
                                'results': f'{j} is required to add a {product_type[:-1]}'}
                    return flask.jsonify(response)

            final_statement += f'insert into {product_type} values (' + ('%s, ' * len(columns_names[product_type]))[
                                                                        :-2] + '); end; $$;'
            final_values += tuple(str(payload[i]) for i in required_input_info[product_type][:-1]) + tuple(
                [str(product_id), version])
        else:
            response = {'status': StatusCodes['bad_request'], 'results': 'Valid type is required to add a product'}
            return flask.jsonify(response)

        # Insert new product info in table products and to the one that corresponds to the same type of product
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
# http://localhost:8080/dbproj/order
##

@app.route('/dbproj/order', methods=['POST'])
def buy_products():
    logger.info('POST /order')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'POST /order - payload: {payload}')

    # If there are more fields than the necessary ones in the request, consider it a bad one
    if len(payload) > 2:
        response = {'status': StatusCodes['bad_request'], 'results': 'Invalid number of fields in the payload'}
        return flask.jsonify(response)

    coupon_id = -1
    discount = 0

    if 'cart' not in payload:
        response = {'status': StatusCodes['bad_request'], 'results': 'cart is required to buy products'}
        return flask.jsonify(response)
    if 'coupon' in payload:
        coupon_id = payload['coupon']

    product_version_statement = 'select version, price, stock from products where product_id = %s and version = (select max(version) from products where product_id = %s)'
    product_quantities_statement = 'insert into product_quantities values (%s, %s, %s, %s)'
    product_stock_statement = 'update products set stock = %s where product_id = %s and version = %s'
    order_statement = 'insert into orders (id, order_date, buyers_users_user_id, coupons_coupon_id, coupons_campaigns_campaign_id) values (%s, %s, %s, %s, %s)'

    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_price = 0.0

    # Get the buyer id
    buyer_id = jwt.decode(flask.request.headers.get('Authorization').split(' ')[1], app.config['SECRET_KEY'],
                          audience=app.config['SESSION_COOKIE_NAME'], algorithms=["HS256"])['user']

    try:
        # Get the order id
        order_id_statement = 'select max(id) from orders '
        cur.execute(order_id_statement, )
        rows = cur.fetchall()

        order_id = rows[0][0] + 1 if rows[0][0] is not None else 1
        order_values = (order_id, order_date, buyer_id, None, None)

        if coupon_id != -1:
            # Get the coupon discount and expiration date and the campaign id that is connected to the coupon with the coupon_id as <coupon_id>
            campaign_statement = 'select campaigns_campaign_id, discount, expiration_date  from coupons, campaigns  where coupon_id = %s and campaigns_campaign_id = campaign_id'
            campaign_values = (coupon_id,)
            cur.execute(campaign_statement, campaign_values)
            rows = cur.fetchall()

            if len(rows) == 0:
                raise CouponNotFound(coupon_id)

            expiration_date = rows[0][2].strftime("%Y-%m-%d")
            today_date = order_date[:-9]
            if expiration_date <= today_date:
                raise CouponExpired(coupon_id, expiration_date, today_date)

            campaign_id = rows[0][0]
            discount = rows[0][1]

            # Create order_values with campaign info
            order_values = tuple(list(order_values)[:-2]) + (coupon_id, campaign_id)

        cur.execute(order_statement, order_values)

        for i in payload['cart']:
            product_quantity = i['quantity']
            product_id = i['product_id']

            product_version_values = (product_id , product_id,)
            cur.execute(product_version_statement, product_version_values)
            rows = cur.fetchall()

            if len(rows) == 0:
                raise ProductNotFound(product_id)

            stock = rows[0][2]
            if stock - product_quantity < 0:
                raise ProductWithoutStockAvailable(product_id, product_quantity, rows[0][2])

            version = rows[0][0].strftime("%Y-%m-%d %H:%M:%S")
            total_price += rows[0][1]

            # Insert in 'product_quantities' table the info about the product that the buyer wanst to buy
            product_quantities_values = (product_quantity, order_id, product_id, version)
            cur.execute(product_quantities_statement, product_quantities_values)

            # Update stock of the product
            product_stock_values = (stock - product_quantity, product_id, version)
            cur.execute(product_stock_statement, product_stock_values)

        # Calculate total_price with the discount (it is 0 if no coupon is applied to the order
        price_discounted = total_price * (discount / 100)
        total_price -= price_discounted

        # Update order info
        order_price_update_statement = 'update orders set price_total = %s where id = %s'
        order_price_update_values = (total_price, order_id,)
        cur.execute(order_price_update_statement, order_price_update_values)

        # Update coupon info
        coupon_statement = 'update coupons set used = true, discount_applied = %s where coupon_id = %s'
        coupon_values = (coupon_id, price_discounted,)
        cur.execute(coupon_statement, coupon_values)

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
        response = {'status': StatusCodes['bad_request'], 'results': 'username and password are required for login'}
        return flask.jsonify(response)

    statement = 'select user_id, username from users where username = %s and password = %s'
    values = (payload['username'], payload['password'])

    try:
        cur.execute(statement, values)
        row = cur.fetchone()

        if row is not None:
            auth_token = jwt.encode({'user': row[0],
                                     'aud': app.config['SESSION_COOKIE_NAME'],
                                     'iat': datetime.utcnow(),
                                     'exp': datetime.utcnow() + timedelta(minutes=10)},
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
