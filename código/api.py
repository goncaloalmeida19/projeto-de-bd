import flask
import logging
import psycopg2
from psycopg2 import sql
import jwt
from cryptography.fernet import Fernet
from datetime import datetime, timedelta

app = flask.Flask(__name__)
app.config['SECRET_KEY'] = 'stordenosvintefachavorpleaseplss'  # 32-character secure key
app.config['SESSION_COOKIE_NAME'] = 'OUR-db-project'
with open('key.txt', 'rb') as keyfile:
    f = Fernet(keyfile.read())

StatusCodes = {
    'success': 200,
    'bad_request': 400,
    'internal_error': 500
}


##########################################################
# EXCEPTIONS
##########################################################

class TokenError(Exception):
    def __init__(self, message='Invalid Authentication Token (session may be expired, try logging in)'):
        super(TokenError, self).__init__(message)


class TokenCreationError(Exception):
    def __init__(self, message='Failed to create user token'):
        super(TokenCreationError, self).__init__(message)


class InvalidAuthenticationException(Exception):
    def __init__(self, message='Incorrect login information'):
        super(InvalidAuthenticationException, self).__init__(message)


class InsufficientPrivilegesException(Exception):
    def __init__(self, privilege, extra_msg='', message='User must be '):
        super(InsufficientPrivilegesException, self).__init__(message + privilege + extra_msg)


class ProductNotFound(Exception):
    def __init__(self, p_id, message='No product found with id: '):
        super(ProductNotFound, self).__init__(message + str(p_id))


class ProductWithoutStockAvailable(Exception):
    def __init__(self, p_id, p_quantity, p_stock,
                 message1="The seller hasn't the required quantity in stock of the product with id '"):
        super(ProductWithoutStockAvailable, self).__init__(
            message1 + str(p_id) + "': Quantity: '" + str(p_quantity) + "' \\ Stock: '" + str(p_stock) + "'")


class AlreadyRated(Exception):
    def __init__(self, p_id, p_version, o_id, message1="Product with id '", message2="' and version '",
                 message3="' from order '", message4="' has already been rated: "):
        super(AlreadyRated, self).__init__(
            message1 + str(p_id) + message2 + p_version + message3 + str(o_id) + message4)


class ParentQuestionNotFound(Exception):
    def __init__(self, question_id, message="Question not found: "):
        super(ParentQuestionNotFound, self).__init__(message + question_id)


class AlreadyInCampaign(Exception):
    def __init__(self, message='Another campaign is already running at that time'):
        super(AlreadyInCampaign, self).__init__(message)


class CampaignExpiredOrNotFound(Exception):
    def __init__(self, message="That campaign doesn't exist or is not available anymore"):
        super(CampaignExpiredOrNotFound, self).__init__(message)


class NoCampaignsFound(Exception):
    def __init__(self, message="No campaigns found"):
        super(NoCampaignsFound, self).__init__(message)


class CouponNotFound(Exception):
    def __init__(self, c_id, message='No Coupon found or user has not subscribed to the coupon with id: '):
        super(CouponNotFound, self).__init__(message + str(c_id))


class CouponAlreadyUsed(Exception):
    def __init__(self, c_id, message='Coupon with id '):
        super(CouponAlreadyUsed, self).__init__(message + str(c_id) + ' already used')


class CouponExpired(Exception):
    def __init__(self, c_id, e_date, t_date, message1="The coupon with id '", message2="' has expired in '"):
        super(CouponExpired, self).__init__(
            message1 + str(c_id) + message2 + e_date + "' and today is '" + t_date + "'")


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

    except jwt.exceptions.InvalidTokenError:
        raise TokenError()

    return user_token['user']


def admin_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()
    try:
        user_id = get_user_id()

        admin_validation = 'select 1 ' \
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

    return user_id


def seller_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()
    try:
        user_id = get_user_id()

        seller_validation = 'select 1 ' \
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


def buyer_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()
    try:
        user_id = get_user_id()

        seller_validation = 'select 1 ' \
                            'from buyers ' \
                            'where users_user_id = %s'

        cur.execute(seller_validation, [user_id])

        if cur.fetchone() is None:
            raise InsufficientPrivilegesException("buyer", fail_msg)

    except (TokenError, InsufficientPrivilegesException) as e:
        raise e

    finally:
        if conn is not None:
            conn.close()

    return user_id


def user_check(fail_msg):
    conn = db_connection()
    cur = conn.cursor()
    try:
        user_id = get_user_id()

        user_validation = 'select 1 ' \
                          'from users ' \
                          'where user_id = %s'

        cur.execute(user_validation, [user_id])

        if cur.fetchone() is None:
            raise InsufficientPrivilegesException("registered", fail_msg)

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
        user='projuser',
        password=f.decrypt(
            b'gAAAAABihXtn404Yx6-DxaYw99HjI_j9pcvteN0EP0a4ZKBzh_mDlp87vHr2NPwB-2u42JAONxCD-e-Mx0Ge8l6A_pnpeb1wdQ==').decode(),
        host='127.0.0.1',
        port='5432',
        database='dbproj'
    )
    return db


##########################################################
# TABLE COLUMNS
##########################################################

columns_names = {
    "users": ['username', 'password', 'email'],
    "ratings": ["comment", "rating", "orders_id", "products_product_id", "products_version", "buyers_users_user_id"],
    "products": ['product_id', 'version', 'name', 'price', 'stock', 'description', 'sellers_users_user_id'],
    "smartphones": ['screen_size', 'os', 'storage', 'color', 'products_product_id', 'products_version'],
    "televisions": ['screen_size', 'screen_type', 'resolution', 'smart', 'efficiency', 'products_product_id',
                    'products_version'],
    "computers": ['screen_size', 'cpu', 'gpu', 'storage', 'refresh_rate', 'products_product_id', 'products_version'],
    "campaigns": ['campaign_id', 'description', 'date_start', 'date_end', 'coupons', 'discount', 'admins_users_user_id']
}


##########################################################
# ENDPOINTS
##########################################################


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

    # logger.debug(f'POST /dbproj/user/ - payload: {payload}')

    required = []

    for field in columns_names["users"]:
        if field not in payload:
            required.append(f'{field} is required for user registry')

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
            admin_check(f" to register {payload['type']}")

        # Get new user_id
        user_id_statement = 'select max(user_id) from users;'
        cur.execute(user_id_statement)
        rows = cur.fetchone()
        user_id = rows[0] + 1  # user 0, platform admin, is expected to always exist

        values = [user_id, payload['username'], payload['password'], payload['email']]
        type_values = [user_id]

        if payload['type'] == 'buyers':
            type_values.append(payload['nif'])
            type_values.append(payload['home_addr'])
        elif payload['type'] == 'sellers':
            type_values.append(payload['nif'])
            type_values.append(payload['shipping_addr'])

        statement = f'insert into users values(%s, %s, %s, %s);'

        type_statement = psycopg2.sql.SQL('insert into {user_type} '
                                          'values(' + '%s, ' * (len(type_values) - 1) + ' %s);'
                                          ).format(user_type=sql.Identifier(payload['type']))

        cur.execute(statement, values)
        cur.execute(type_statement, type_values)

        conn.commit()
        response = {'status': StatusCodes['success'], 'results': user_id}

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

    # logger.debug(f'PUT /dbproj/user/ - payload: {payload}')

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
                                     'exp': datetime.utcnow() + timedelta(minutes=20)},
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


##
# Create a new product with a JSON payload
##
# To use it, access through postman:
##
# POST http://localhost:8080/dbproj/product
##
@app.route('/dbproj/product', methods=['POST'])
def add_product():
    logger.info('POST /dbproj/product')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # The type of the product is essential
    required_input_info = {}
    for item, value in columns_names.items():
        if item not in ["users", "ratings", "campaigns"]:
            if item == "products":
                required_input_info[item] = value[2:-1] + ["type"]
            else:
                required_input_info[item] = value[:-2]

    # logger.debug(f'POST /product - required_product_input_info: {required_input_info}')

    # Verification of the required fields to add a product
    for i in required_input_info["products"]:
        if i not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': f'{i} is required to add a product'}
            if conn is not None:
                conn.close()
            return flask.jsonify(response)

    product_type = payload['type']

    try:
        # Get the seller id
        seller_id = seller_check(" to add a new product")

        # Get new product_id
        product_id_statement = 'select max(product_id) from products;'
        product_id_values = (seller_id,)
        cur.execute(product_id_statement, product_id_values)
        rows = cur.fetchall()
        product_id = rows[0][0] + 1 if rows[0][0] is not None else 1

        version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(version)

        product_statement = 'insert into products values (%s, %s, %s, %s, %s, %s, %s);'
        product_values = (
            product_id, version, payload['name'], payload['price'], payload['stock'], payload['description'], seller_id)

        # Insert new product info in table products
        cur.execute(product_statement, product_values)

        # Statement and values about the info that will be inserted
        # in the table that corresponds to the same type of product
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

            product_type_values = tuple(payload[i] for i in required_input_info[product_type]) \
                                  + tuple([product_id, version])

        else:
            response = {'status': StatusCodes['bad_request'], 'results': 'Valid type is required to add a product'}
            return flask.jsonify(response)

        # Insert new product info to the one that corresponds to the same type of product
        cur.execute(product_type_statement, product_type_values)

        # Response of the adding the product status
        response = {'status': StatusCodes['success'], 'results': f'{product_id}'}

        # commit the transaction
        conn.commit()

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'POST /dbproj/product - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/product - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Update a product with a JSON payload
##
# To use it, access through postman:
##
# http://localhost:8080/dbproj/product/69420
##
@app.route('/dbproj/product/<product_id>', methods=['PUT'])
def update_product(product_id):
    logger.info('PUT /dbproj/product/<product_id>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'PUT /dbproj/product/<product_id> - payload: {payload}')

    version = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    type_statement = 'select gettype(%s);'
    type_values = (product_id,)

    try:
        seller_check(" to update a product")

        cur.execute(type_statement, type_values)
        product_type = cur.fetchall()[0][0]
        if product_type == 'invalid':
            raise ProductNotFound(product_id)

        for i in payload:
            if i not in columns_names['products'] and i not in columns_names[product_type]:
                response = {'status': StatusCodes['bad_request'], 'results': f'{i} is not a valid attribute'}
                if conn is not None:
                    conn.close()
                return flask.jsonify(response)

        non_changed = list(set(columns_names[product_type] + columns_names['products']) - set(payload.keys()))

        '''
        non_changed_items_statement = f'select {",".join(non_changed)} from products,{product_type} ' \
                                      'where product_id = %s ' \
                                      'and version =(select max(version) from products where product_id = %s) ' \
                                      'and products_product_id = product_id and version = products_version'
        '''

        # get the data of the old version of the product
        non_changed_items_statement = psycopg2.sql.SQL(
            f'select {",".join(non_changed)} '
            'from products, {prod_type} '
            'where product_id = %s '
            'and version =(select max(version) from products where product_id = %s) '
            'and products_product_id = product_id and version = products_version;'
        ).format(prod_type=sql.Identifier(product_type))

        non_changed_items_values = (product_id, product_id,)

        cur.execute(non_changed_items_statement, non_changed_items_values)
        results = cur.fetchall()[0]

        # change the data, creating a new version
        new_data_products = tuple([payload[i] if i in list(payload.keys())
                                   else version if i == 'version' else results[non_changed.index(i)]
                                   for i in columns_names['products']])

        new_data_product_type = tuple([payload[i] if i in list(payload.keys())
                                       else version if i == 'products_version' else results[non_changed.index(i)]
                                       for i in columns_names[product_type]])

        # add the new version to the products table and corresponding product type table
        insert_products_statement = f'insert into products values({("%s," * len(columns_names["products"]))[:-1]});'

        insert_product_type_statement = psycopg2.sql.SQL('insert into {prod_type} '
                                                         f'values({("%s," * len(columns_names[product_type]))[:-1]});'
                                                         ).format(prod_type=sql.Identifier(product_type))

        cur.execute(insert_products_statement, new_data_products)
        cur.execute(insert_product_type_statement, new_data_product_type)

        response = {'status': StatusCodes['success']}
        conn.commit()

    except (TokenError, InsufficientPrivilegesException, ProductNotFound) as error:
        logger.error(f'PUT /product/<product_id> - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'PUT /product/<product_id> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Perform an order with a JSON payload
##
# To use it, access through postman:
##
# http://localhost:8080/dbproj/order
##
@app.route('/dbproj/order', methods=['POST'])
def buy_products():
    logger.info('POST /dbproj/order')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'POST /dbproj/order - payload: {payload}')

    # If there are more fields than the necessary ones in the request, consider it a bad one
    if len(payload) > 2:
        response = {'status': StatusCodes['bad_request'], 'results': 'Invalid number of fields in the payload'}
        if conn is not None:
            conn.close()
        return flask.jsonify(response)

    coupon_id = -1 if 'coupon' not in payload else payload['coupon']
    discount = 0

    if 'cart' not in payload:
        response = {'status': StatusCodes['bad_request'],
                    'results': 'cart listing items and quantities is required to buy products'}
        if conn is not None:
            conn.close()
        return flask.jsonify(response)

    product_version_statement = 'select version, price, stock from products where product_id = %s and version = (select max(version) from products where product_id = %s);'
    product_quantities_statement = 'insert into product_quantities values (%s, %s, %s, %s);'
    product_stock_statement = 'update products set stock = %s where product_id = %s and version = %s;'
    order_statement = 'insert into orders (id, order_date, buyers_users_user_id, coupons_coupon_id, coupons_campaigns_campaign_id) values (%s, %s, %s, %s, %s);'

    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_price = 0.0

    try:
        # Get the buyer id
        buyer_id = buyer_check(" to perform an order")

        # Get the order id
        order_id_statement = 'select max(id) from orders '
        cur.execute(order_id_statement)
        rows = cur.fetchall()

        order_id = rows[0][0] + 1 if rows[0][0] is not None else 1
        order_values = (order_id, order_date, buyer_id, None, None)

        if coupon_id != -1:
            # Get the coupon discount and expiration date and the campaign id that is connected to the coupon with the coupon_id as <coupon_id>
            campaign_statement = 'select campaigns_campaign_id, discount, expiration_date, used ' \
                                 'from coupons, campaigns  ' \
                                 'where coupon_id = %s and campaigns_campaign_id = campaign_id and buyers_users_user_id = %s;'
            campaign_values = (coupon_id, buyer_id)
            cur.execute(campaign_statement, campaign_values)
            rows = cur.fetchall()

            if len(rows) == 0:
                raise CouponNotFound(coupon_id)

            if rows[0][3]:
                raise CouponAlreadyUsed(coupon_id)

            expiration_date = rows[0][2]
            today_date = order_date[:-9]
            if datetime.strptime(today_date, "%Y-%m-%d").date() >= expiration_date:
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

            print(rows)

            if len(rows) == 0:
                raise ProductNotFound(product_id)

            stock = rows[0][2]
            if stock - product_quantity < 0:
                raise ProductWithoutStockAvailable(product_id, product_quantity, rows[0][2])

            version = rows[0][0].strftime("%Y-%m-%d %H:%M:%S")
            total_price += rows[0][1]

            # Insert in 'product_quantities' table the info about the product that the buyer wants to buy
            product_quantities_values = (product_quantity, order_id, product_id, version)
            cur.execute(product_quantities_statement, product_quantities_values)

            # Update stock of the product
            product_stock_values = (stock - product_quantity, product_id, version)
            cur.execute(product_stock_statement, product_stock_values)

        # Calculate total_price with the discount (0 if no coupon is applied to the order) and update order info
        order_price_update_statement = 'update orders set price_total = %s - (%s * (%s / 100)) where id = %s;'
        order_price_update_values = (total_price, total_price, discount, order_id,)
        cur.execute(order_price_update_statement, order_price_update_values)

        # Update coupon info
        coupon_statement = 'update coupons set used = true, discount_applied = %s * (%s / 100) where coupon_id = %s;'
        coupon_values = (total_price, discount, coupon_id,)
        cur.execute(coupon_statement, coupon_values)

        response = {'status': StatusCodes['success'], 'results': f'{order_id}'}
        conn.commit()

    except (TokenError, InsufficientPrivilegesException, ProductNotFound, ProductWithoutStockAvailable, CouponNotFound,
            CouponExpired) as error:
        logger.error(f'POST /dbproj/order - error: {error}')
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


##
# Give rating/feedback on a product with a JSON payload
##
# To use it, access through postman:
##
# http://localhost:8080/dbproj/rating/69420
##
@app.route('/dbproj/rating/<product_id>', methods=['POST'])
def give_rating_feedback(product_id):
    logger.info('POST /dbproj/rating/<product_id>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'POST /dbproj/rating/<product_id> - payload: {payload}')

    # If there are more fields than the necessary ones in the request, consider it a bad one
    if len(payload) > 2:
        response = {'status': StatusCodes['bad_request'],
                    'results': 'Invalid number of fields in the payload; should be 2'}
        return flask.jsonify(response)

    # Verification of the required fields to do a rating to a product
    for i in columns_names["ratings"][:2]:
        if i not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': f'{i} is required to rate a product'}
            if conn is not None:
                conn.close()
            return flask.jsonify(response)

    # A rating needs to be between 1 and 5 if not consider the request a bad one
    if not 1 <= payload['rating'] <= 5:
        response = {'status': StatusCodes['bad_request'], 'results': f'Product rating must be between 1 and 5'}
        if conn is not None:
            conn.close()
        return flask.jsonify(response)

    try:
        # Get the buyer id
        buyer_id = buyer_check(" to perform a purchase")

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

        # Verify if the product has already been rated
        statement = 'select exists (select rating, comment from ratings where orders_id = %s and products_product_id = %s) ' \
                    'from ratings ' \
                    'where orders_id = %s ' \
                    'and products_product_id = %s'
        values = (order_id, product_id, order_id, product_id)
        cur.execute(statement, values)
        rows = cur.fetchall()

        if len(rows) != 0:
            raise AlreadyRated(product_id, version, order_id)

        # Insert the rating info in the "ratings" table and update the average rating of a product
        statement = 'insert into ratings values (%s, %s, %s, %s, %s, %s); '
        values = (payload['comment'], payload['rating'], order_id, product_id, version, buyer_id)
        cur.execute(statement, values)

        # Response of the rating status
        response = {'status': StatusCodes['success']}

        # commit the transaction
        conn.commit()

    except (TokenError, InsufficientPrivilegesException, ProductNotFound, AlreadyRated, CouponExpired) as error:
        logger.error(f'POST /dbproj/rating/<product_id> - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /dbproj/rating/<product_id> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Post a question about a product, or reply to a question, with a JSON payload
##
# To use it, access through postman:
##
# Post question:
# POST http://localhost:8080/dbproj/<product_id>
##
# Reply to a question:
# POST http://localhost:8080/dbproj/<product_id>/<parents_question_id>
##
@app.route('/dbproj/questions/<product_id>', methods=['POST'])
@app.route('/dbproj/questions/<product_id>/<parents_question_id>', methods=['POST'])
def post_question(product_id=None, parents_question_id=None):
    if parents_question_id is None:
        logger.info('PUT /dbproj/questions/<product_id>')
    else:
        logger.info('PUT /dbproj/questions/<product_id>/<parents_question_id>')

    payload = flask.request.get_json()

    '''
    if parents_question_id is None:
        logger.debug(f'POST /dbproj/questions/<product_id> - payload: {payload}')
    else:
        logger.debug(f'POST /dbproj/questions/<product_id>/<parents_question_id> - payload: {payload}')
    '''

    conn = db_connection()
    cur = conn.cursor()

    if 'question' not in payload:
        response = {'status': StatusCodes['bad_request'],
                    'results': 'question must be provided for posting about a product'}
        return flask.jsonify(response)

    try:
        statement = 'select * from ' \
                    '(select max(question_id) from questions where products_product_id = %s) as q_ids, ' \
                    '(select max(version) from products where product_id = %s) as p_vers;'
        cur.execute(statement, [product_id, product_id])
        rows = cur.fetchone()

        if rows[1] is None:
            raise ProductNotFound(product_id)

        products_version = rows[1].strftime("%Y-%m-%d %H:%M:%S")

        question_id = rows[0] + 1 if rows[0] is not None else 1

        insert_question_values = [question_id, payload['question'], user_check(" to post a question about a product"),
                                  product_id, products_version]

        if parents_question_id is not None:

            parent_question_statement = 'select users_user_id ' \
                                        'from questions where products_product_id = %s and question_id = %s;'
            parent_question_values = [product_id, parents_question_id]

            cur.execute(parent_question_statement, parent_question_values)
            parent_question_rows = cur.fetchone()

            print(parent_question_rows)

            if parent_question_rows is None:
                raise ParentQuestionNotFound(parents_question_id)

            insert_question_values.extend([parents_question_id, parent_question_rows[0]])

        insert_question_statement = f'insert into questions ' \
                                    f'(question_id, question_text, users_user_id, products_product_id, products_version ' \
                                    f'{", questions_question_id, questions_users_user_id" if parents_question_id is not None else ""}) ' \
                                    f'values (%s, %s, %s, %s, %s{", %s, %s" if parents_question_id is not None else ""});'

        cur.execute(insert_question_statement, insert_question_values)

        response = {'status': StatusCodes['success'], 'results': question_id}
        conn.commit()

    except (TokenError, InsufficientPrivilegesException, ProductNotFound) as error:
        logger.error(f'PUT /dbproj/questions/ - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'PUT /dbproj/questions/ - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Get notifications
##
# To use it, access through postman:
##
# GET http://localhost:8080/dbproj/inbox
##
@app.route('/dbproj/inbox', methods=['GET'])
def get_notifications():
    logger.info('GET /dbproj/inbox')

    conn = db_connection()
    cur = conn.cursor()

    try:
        user_id = user_check(" to see notification inbox")

        statement = 'select time, notification_id, content from notifications where users_user_id = %s'
        values = [user_id]

        cur.execute(statement, values)
        notifications = cur.fetchall()

        response = {'status': StatusCodes['success'], 'results': notifications}
        conn.commit()

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'GET /dbproj/inbox - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /dbproj/inbox - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Obtain information about a product with product_id <product_id>
##
# To use it, access through Postman:
##
# GET http://localhost:8080/dbproj/products/7390626
##
@app.route('/dbproj/product/<product_id>', methods=['GET'])
def get_product_info(product_id):
    logger.info('GET /dbproj/product/<product_id>')
    conn = db_connection()
    cur = conn.cursor()

    try:
        user_check(" to get product info")

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
            comments_rating = ["Product hasn't been rated yet",
                               "Product without comments because it hasn't been rated yet"]

        content = {'name': rows[0][0], 'stock': rows[0][1], 'description': rows[0][2], 'prices': rows[0][3].split(','),
                   'rating': comments_rating[0], 'comments': comments_rating[1].split(',')}

        # Response of the status of obtaining a product and the information obtained
        response = {'status': StatusCodes['success'], 'results': content}

    except (TokenError, InsufficientPrivilegesException, ProductNotFound) as error:
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
# Obtain monthly statistics about sales of the last year
##
# To use it, access through postman:
##
# GET http://localhost:8080/dbproj/report/year
##
@app.route('/dbproj/report/year', methods=['GET'])
def get_stats():
    logger.info('GET /dbproj/report/year')

    conn = db_connection()
    cur = conn.cursor()

    statement = 'select  to_char(order_date, \'MM-YYYY\') as month, round(cast(sum(price_total) as numeric), 2), count(id) ' \
                'from orders ' \
                'where order_date > (CURRENT_DATE - interval \'1 year\') ' \
                'group by month;'

    try:
        user_check(" to obtain sale stats")

        cur.execute(statement)
        rows = cur.fetchall()

        sale_stats = [{'month': r[0], 'total_value': r[1], 'orders': r[2]} for r in rows]

        # print(sale_stats)  # debug

        response = {'status': StatusCodes['success'], 'results': sale_stats}

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'/dbproj/report/year - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'/dbproj/report/year - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Create new coupon campaing
##
# To use it, access through postman:
##
# GET http://localhost:8080/dbproj/campaign/
##
@app.route('/dbproj/campaign/', methods=['POST'])
def add_campaign():
    logger.info('POST /dbproj/campaign/')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # logger.debug(f'POST /dbproj/campaign/ - payload: {payload}')

    # Validate fields
    for i in payload:
        if i not in columns_names['campaigns'][1:6]:
            response = {'status': StatusCodes['bad_request'],
                        'errors': f'{i} is not a valid attribute'}
            if conn is not None:
                conn.close()
            return flask.jsonify(response)
    for i in range(1, 6):
        if columns_names['campaigns'][i] not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'errors': f'{columns_names["campaigns"][i]} value not in payload'}
            if conn is not None:
                conn.close()
            return flask.jsonify(response)

    if datetime.strptime(payload['date_start'], "%Y-%m-%d") > datetime.strptime(payload['date_end'], "%Y-%m-%d"):
        response = {'status': StatusCodes['bad_request'],
                    'errors': 'The end date must be after the start date'}
        if conn is not None:
            conn.close()
        return flask.jsonify(response)

    verify_dates_statement = 'select exists(select 1 from campaigns where %s <= date_end and %s >= date_start);'
    verify_dates_values = (payload['date_start'], payload['date_end'])

    campaign_id_statement = 'select coalesce(max(campaign_id), 0) + 1 from campaigns;'

    campaign_statement = f'insert into campaigns ' \
                         f'values (%s,%s,%s,%s,%s,%s,%s);'

    try:
        admin_id = admin_check(" to create a campaign")

        cur.execute(verify_dates_statement, verify_dates_values)
        if cur.fetchall()[0][0]:
            raise AlreadyInCampaign

        cur.execute(campaign_id_statement)
        campaign_id = cur.fetchone()[0]

        campaign_values = tuple([campaign_id] + list(payload.values()) + [admin_id])

        cur.execute(campaign_statement, campaign_values)

        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'{campaign_id}'}

    except (AlreadyInCampaign, TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'POST /campaign - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /campaign - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
# Subscribe to coupon campaign
##
# To use it, access through postman:
##
# GET http://localhost:8080/dbproj/subscribe/<campaign_id>
##
@app.route('/dbproj/subscribe/<campaign_id>', methods=['PUT'])
def subscribe_campaign(campaign_id):
    logger.info('PUT /dbproj/subscribe/<campaign_id>')

    conn = db_connection()
    cur = conn.cursor()

    time_now = datetime.now()
    expiration_date = time_now + timedelta(days=30)

    time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
    expiration_date = expiration_date.strftime("%Y-%m-%d %H:%M:%S")

    # campaign_expired_statement = 'select exists(select 1 from campaigns )'

    subscribe_statement = 'update campaigns set coupons = coupons - 1 ' \
                          'where campaign_id = %s and %s between date_start and date_end and coupons > 0;'
    subscribe_values = (campaign_id, time_now)

    gen_coupon_statement = 'select coalesce(max(coupon_id) + 1, 1) from coupons;'

    insert_coupon_statement = f'insert into coupons (coupon_id, used, discount_applied, expiration_date, campaigns_campaign_id, buyers_users_user_id) values (%s,%s,%s,%s,%s,%s);'

    try:
        user_id = buyer_check(" to subscribe to coupon campaign")

        cur.execute(subscribe_statement, subscribe_values)
        if cur.rowcount == 0:
            raise CampaignExpiredOrNotFound

        cur.execute(gen_coupon_statement)
        coupon_id = cur.fetchall()[0][0]

        insert_coupon_values = (coupon_id, 'false', 0, expiration_date, campaign_id, user_id)
        cur.execute(insert_coupon_statement, insert_coupon_values)

        response = {'status': StatusCodes['success'],
                    'results': {'coupon_id': coupon_id, 'expiration_date': expiration_date}}

        conn.commit()

    except (TokenError, InsufficientPrivilegesException, CampaignExpiredOrNotFound) as error:
        logger.error(error)
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


##
# Obtain campaign statistics
##
# To use it, access through postman:
##
# GET http://localhost:8080/dbproj/report/campaign
##
@app.route('/dbproj/report/campaign', methods=['GET'])
def get_campaign_stats():
    logger.info('GET /dbproj/report/campaign')

    conn = db_connection()
    cur = conn.cursor()

    stats_statement = "select campaign_id," \
                      "(select count(*) from coupons where campaigns_campaign_id = campaign_id)," \
                      "(select count(*) from coupons where campaigns_campaign_id = campaign_id and used = 'true')," \
                      "(select coalesce(sum(discount_applied),0) from coupons where campaigns_campaign_id = campaign_id) " \
                      "from campaigns group by campaign_id"

    try:
        user_check(" to obtain campaign stats")

        cur.execute(stats_statement)
        rows = cur.fetchall()
        if not rows:
            raise NoCampaignsFound

        # logger.debug('GET /report/campaign - parse')
        results = []
        for row in rows:
            # logger.debug(row)
            content = {'campaign_id': int(row[0]), 'generated_coupons': int(row[1]),
                       'used_coupons': int(row[2]), 'total_discount_value': float(row[3])}
            results.append(content)  # appending to the payload to be returned

        response = {'status': StatusCodes['success'], 'results': results}

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'GET /report/campaign - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /report/campaign - error: {error}')
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
