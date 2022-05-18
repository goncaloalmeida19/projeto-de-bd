from datetime import datetime, timedelta
import flask
import logging
import jwt
import psycopg2
from psycopg2 import sql

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
    "campaigns": ['campaign_id', 'description', 'date_start', 'date_end', 'coupons', 'discount',
                  'admins_users_user_id'],
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
    def __init__(self, privilege, extra_msg='', message='User must be '):
        super(InsufficientPrivilegesException, self).__init__(message + privilege + extra_msg)


class ParentQuestionNotFound(Exception):
    def __init__(self, question_id, message="Question not found: "):
        super(ParentQuestionNotFound, self).__init__(message + question_id)


class ProductNotFound(Exception):
    def __init__(self, p_id, message='No product found with id: '):
        super(ProductNotFound, self).__init__(message + str(p_id))


class ProductWithoutStockAvailable(Exception):
    def __init__(self, p_id, p_quantity, p_stock,
                 message1="The seller hasn't the required quantity in stock of the product with id '"):
        super(ProductWithoutStockAvailable, self).__init__(
            message1 + str(p_id) + "': Quantity: '" + str(p_quantity) + "' \\ Stock: '" + str(p_stock) + "'")


class CouponNotFound(Exception):
    def __init__(self, c_id, message='No coupon found with id: '):
        super(CouponNotFound, self).__init__(message + str(c_id))


class CouponExpired(Exception):
    def __init__(self, c_id, e_date, t_date, message1="The coupon with id '", message2="' has expired in '"):
        super(CouponExpired, self).__init__(
            message1 + str(c_id) + message2 + e_date + "' and today is '" + t_date + "'")


# Product from order x already been rated
class AlreadyRated(Exception):
    def __init__(self, p_id, p_version, o_id, message1="Product with id '", message2="' and version '",
                 message3="' from order '", message4="' already been rated: "):
        super(AlreadyRated, self).__init__(
            message1 + str(p_id) + message2 + p_version + message3 + str(o_id) + message4)


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
            raise InsufficientPrivilegesException("admin ", fail_msg)

    except (TokenError, InsufficientPrivilegesException) as e:
        raise e

    finally:
        if conn is not None:
            conn.close()


def seller_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()
    try:
        user_id = get_user_id()

        seller_validation = 'select users_user_id ' \
                            'from sellers ' \
                            'where users_user_id = %s'

        cur.execute(seller_validation, [user_id])

        if cur.fetchone() is None:
            raise InsufficientPrivilegesException("seller", fail_msg)

    except (TokenError, InsufficientPrivilegesException) as e:
        raise e

    finally:
        if conn is not None:
            conn.close()

    return user_id


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
    logger.info('GET /dbproj/products/<product_id>')
    conn = db_connection()
    cur = conn.cursor()

    try:
        # Get info about the product that have the product_id correspondent to the one given
        statement = 'select name, stock, description, ' \
                    "(select string_agg(price || ' - ' || version, ',') from products where product_id = %s), " \
                    "(select concat(avg(rating)::float,';',string_agg(comment,',')) from ratings where products_product_id = %s) " \
                    'from products ' \
                    'where product_id = %s and version = (select max(version) from products where product_id = %s) '
        values = (product_id,) * 4
        cur.execute(statement, values)
        rows = cur.fetchall()

        if len(rows) == 0:
            raise ProductNotFound(product_id)

        comments_rating = rows[0][4].split(';')

        if len(comments_rating[0]) == 0:
            comments_rating = ["Product wasn't rated yet", "Product without comments because it wasn't rated yet"]

        content = {'name': rows[0][0], 'stock': rows[0][1], 'description': rows[0][2], 'prices': rows[0][3].split(','),
                   'rating': comments_rating[0], 'comments': comments_rating[1].split(',')}

        # Response of the status of obtaining a product and the information obtained
        response = {'status': StatusCodes['success'], 'results': content}

    except ProductNotFound as error:
        logger.error(f'GET /dbproj/product/<product_id> - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /dbproj/product/<product_id> - error: {error}')
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
    logger.info('POST /dbproj/rating/<product_id>')
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

    try:
        # Get the buyer id
        buyer_id = get_user_id()

        # Get info about the product that will be rated (the one already bought)
        statement = 'select orders.id, product_quantities.products_version ' \
                    'from product_quantities, orders ' \
                    'where product_quantities.products_product_id = %s ' \
                    'and product_quantities.orders_id = orders.id ' \
                    'and orders.buyers_users_user_id = %s '
        values = (product_id, buyer_id,)
        cur.execute(statement, values)
        rows = cur.fetchall()

        if len(rows) == 0:
            raise ProductNotFound(product_id)

        # Get the most recent order id related to the most recent version of the product with id <product_id>
        order_id = rows[len(rows) - 1][0]
        version = rows[len(rows) - 1][1].strftime("%Y-%m-%d %H:%M:%S")

        # Verify if the product have already been rated
        statement = 'select exists (select rating, comment from ratings where orders_id = %s and products_product_id = %s) ' \
                    'from ratings ' \
                    'where orders_id = %s ' \
                    'and products_product_id = %s'
        values = (order_id, product_id, order_id, product_id,)
        cur.execute(statement, values)
        rows = cur.fetchall()

        if rows[0][0]:
            raise AlreadyRated(product_id, version, order_id)

        # Insert the rating info in the "ratings" table and update the average rating of a product
        statement = 'insert into ratings values (%s, %s, %s, %s, %s, %s); '
        values = (
            payload['comment'], payload['rating'], order_id, product_id, version, buyer_id)
        cur.execute(statement, values)

        # Response of the rating status
        response = {'status': StatusCodes['success']}

        # commit the transaction
        conn.commit()

    except (ProductNotFound, AlreadyRated, CouponExpired, TokenError) as error:
        logger.error(f'POST /dbproj/rating/<product_id> - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/rating/<product_id> - error: {error}')
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
    logger.info('POST /dbproj/product')
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
                        'results': f'{i} is required to add a product'}
            return flask.jsonify(response)

    product_type = payload['type']

    try:
        # Get the seller id
        seller_id = get_user_id()

        # Get new product_id
        product_id_statement = 'select max(product_id) from products where sellers_users_user_id = %s'
        product_id_values = (seller_id,)
        cur.execute(product_id_statement, product_id_values)
        rows = cur.fetchall()
        product_id = rows[0][0] + 1 if rows[0][0] is not None else 1

        product_statement = 'insert into products values (%s, %s, %s, %s, %s, %s); '
        product_values = (
            str(product_id), payload['name'], str(payload['price']), str(payload['stock']),
            payload['description'],
            str(seller_id))

        # Insert new product info in table products
        cur.execute(product_statement, product_values)

        # Statement and values about the info that will be insert to the table that corresponds to the same type of product
        if product_type in list(columns_names)[2:-1]:
            for j in required_input_info[product_type]:
                if j not in payload:
                    response = {'status': StatusCodes['bad_request'],
                                'results': f'{j} is required to add a {product_type[:-1]}'}
                    return flask.jsonify(response)

            product_type_statement = psycopg2.sql.SQL(
                'insert into {product_type} ' +
                f'values ({("%s, " * len(columns_names[product_type]))[:-2]});'
            ).format(product_type=sql.Identifier(product_type))

            product_type_values = tuple(str(payload[i]) for i in required_input_info[product_type][:-1]) + tuple(
                [str(product_id)])
        else:
            response = {'status': StatusCodes['bad_request'], 'results': 'Valid type is required to add a product'}
            return flask.jsonify(response)

        # Insert new product info to the one that corresponds to the same type of product
        cur.execute(product_type_statement, product_type_values)

        # Response of the adding the product status
        response = {'status': StatusCodes['success'], 'results': f'{product_id}'}

        # commit the transaction
        conn.commit()

    except (CouponExpired, TokenError) as error:
        logger.error(f'POST /dbproj/product - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/product - error: {error}')
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
    logger.info('POST /dbproj/order')
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

    try:
        # Get the buyer id
        buyer_id = get_user_id()

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

            expiration_date = rows[0][2]
            today_date = order_date[:-9]
            if datetime.strptime(today_date, "%Y-%m-%d") >= expiration_date:
                raise CouponExpired(coupon_id, expiration_date, today_date)

            campaign_id = rows[0][0]
            discount = rows[0][1]

            # Create order_values with campaign info
            order_values = tuple(list(order_values)[:-2]) + (coupon_id, campaign_id)

        cur.execute(order_statement, order_values)

        for i in payload['cart']:
            product_quantity = i['quantity']
            product_id = i['product_id']

            product_version_values = (product_id, product_id,)
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

        # Calculate total_price with the discount (it is 0 if no coupon is applied to the order and update order info
        order_price_update_statement = 'update orders set price_total = %s - (%s * (%s / 100)) where id = %s'
        order_price_update_values = (total_price, total_price, discount, order_id,)
        cur.execute(order_price_update_statement, order_price_update_values)

        # Update coupon info
        coupon_statement = 'update coupons set used = true, discount_applied = %s * (%s / 100) where coupon_id = %s'
        coupon_values = (total_price, discount, coupon_id,)
        cur.execute(coupon_statement, coupon_values)

        response = {'status': StatusCodes['success'], 'results': f'{order_id}'}

        # commit the transaction
        conn.commit()

    except (ProductNotFound, ProductWithoutStockAvailable, CouponNotFound, CouponExpired, TokenError) as error:
        logger.error(f'POST /dbproj/order - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
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

        # logger.debug('GET /users - parse')
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


##
# Register a user with a JSON payload
##
# To use it, access through Postman:
##
# POST http://localhost:8080/dbproj/user
##
@app.route('/dbproj/user/', methods=['POST'])
def register_user():
    logger.info('POST /dbproj/user/')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /users - payload: {payload}')

    required = []

    if 'user_id' not in payload:
        required.append('user_id is required for user registry')

    if 'username' not in payload:
        required.append('username is required for user registry')

    if 'password' not in payload:
        required.append('password is required for user registry')

    if 'type' not in payload or payload['type'] not in ['buyers', 'sellers', 'admins']:
        required.append('user type is required for user registry: \'buyers\', \'sellers\' or \'admins\'')

    elif payload['type'] == 'buyers' or payload['type'] == 'sellers':
        if 'nif' not in payload:
            required.append('nif is required to register buyers and sellers')

        if 'home_addr' not in payload and 'shipping_addr' not in payload:
            required.append('address (home_addr or shipping_addr) is required to register buyers or sellers')

    if len(required) > 0:
        response = {'status': StatusCodes['bad_request'], 'errors': required}
        return flask.jsonify(response)

    try:
        if payload['type'] != 'buyers' and (payload['type'] == 'sellers' or payload['type'] == 'admins'):
            admin_check(f"to register {payload['type']}")

        values = [payload['user_id'], payload['username'], payload['password']]
        extra_values = [payload['user_id']]

        if 'email' in payload:
            values.append(payload['email'])

        if payload['type'] == 'buyers':
            extra_values.append(payload['nif'])
            extra_values.append(payload['home_addr'])
        elif payload['type'] == 'sellers':
            extra_values.append(payload['nif'])
            extra_values.append(payload['shipping_addr'])

        statement = psycopg2.sql.SQL(
            f'insert into users (user_id, username, password) values (%s, %s, %s{", %s;" if "email" in payload else ""});'
            + ' insert into {type} values (' + '%s, ' * (len(extra_values) - 1) + ' %s);'
        ).format(type=sql.Identifier(payload['type']))

        print(statement)
        values.extend(extra_values)

        cur.execute(statement, values)

        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Registered user {payload["username"]}'}

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'POST /dbproj/user/ - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/user/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Perform user login with a JSON payload
##
# To use it, access through Postman:
##
# PUT http://localhost:8080/dbproj/user
##
@app.route('/dbproj/user/', methods=['PUT'])
def login_user():
    logger.info('PUT /dbproj/user/')

    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /users - payload: {payload}')

    if 'username' not in payload or 'password' not in payload:
        response = {'status': StatusCodes['bad_request'], 'errors': 'username and password are required for login'}
        return flask.jsonify(response)

    statement = 'select user_id, username from users where username = %s and password = %s;'
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

        response = {'status': StatusCodes['success'], 'token': auth_token}

        conn.commit()

    except InvalidAuthenticationException as error:
        logger.error(f'PUT /dbproj/user/ {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'PUT /dbproj/user/ {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


@app.route('/dbproj/product/<product_id>', methods=['PUT'])
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
        product_type = cur.fetchall()[0][0]  # TODO EXCEÇÃO -> ProductNotFound

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
                                   else version if i == 'version' else results[columns_names['products'].index(i)]
                                   for i in columns_names['products']])
        new_data_product_type = tuple([payload[i] if i in list(payload.keys())
                                       else version if i == 'products_version' else results[
            columns_names[product_type].index(i) + len(columns_names['products'])]
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
