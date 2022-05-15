import flask
import logging
import psycopg2
from psycopg2 import sql
import jwt
from datetime import datetime, timedelta

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
    def __init__(self, privilege, extra_msg='', message='User must be '):
        super(InsufficientPrivilegesException, self).__init__(message + privilege + extra_msg)


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

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
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
    logger.info('POST /product')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    # The type of the product is essential
    required_input_info = dict(
        (item, value[:-2] + ['type']) if item not in ["products", "ratings", "campaigns"]
        else (item, value[2: -1]) for item, value in columns_names.items())

    # logger.debug(f'POST /product - required_product_input_info: {required_product_input_info}')

    # Verification of the required fields to add a product
    for i in required_input_info["products"]:
        if i not in payload:
            response = {'status': StatusCodes['bad_request'],
                        'results': f'{i.capitalize()} is required to add a product'}
            return flask.jsonify(response)

    version = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    product_type = payload['type']

    try:
        # Get the seller id
        seller_id = seller_check("to add a new product")

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

        # Statement and values about the info that will be inserted in the table that corresponds to the same type of product
        if product_type in list(columns_names)[2:-1]:
            for j in required_input_info[product_type]:
                if j not in payload:
                    response = {'status': StatusCodes['bad_request'],
                                'results': f'{j} is required to add a {product_type[:-1]}'}
                    return flask.jsonify(response)

            '''
            final_statement += f'insert into {product_type} ' \
                               f'values ({("%s, " * len(columns_names[product_type]))[:-2]}); end; $$;'
            '''

            # TODO: testar
            final_statement = psycopg2.sql.SQL(
                'insert into {product_type} ' +
                f'values ({("%s, " * len(columns_names[product_type]))[:-2]}); end; $$;'
            ).format(product_type=sql.Identifier(product_type))

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

    except (TokenError, InsufficientPrivilegesException) as error:
        logger.error(f'GET /users - error: {error}')
        response = {'status': StatusCodes['bad_request'], 'errors': str(error)}
        conn.rollback()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# TODO: update product details

# TODO: perform order

# TODO: leave rating/feedback

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

    conn = db_connection()
    cur = conn.cursor()

    if 'question' not in payload:
        response = {'status': StatusCodes['bad_request'],
                    'results': 'question must be provided for posting about a product'}
        return flask.jsonify(response)

    try:
        # statement = 'select max(question_id), max(version) from questions, products where product_id = %s and products_product_id = product_id;'
        statement = 'select * from (select max(question_id) from questions where products_product_id = %s) as q_ids, (select max(version) from products where product_id = %s) as p_vers';
        cur.execute(statement, [product_id, product_id])
        rows = cur.fetchone()

        if rows[1] is None:
            raise ProductNotFound(product_id)

        products_version = rows[1].strftime("%Y-%m-%d %H:%M:%S")

        if rows[0] is None:
            # first question made about this product
            question_id = 0
        else:
            question_id = rows[0] + 1

        # insert_question_values = [question_id, payload['question'], get_user_id(), notification_id, product_id, products_version]
        insert_question_values = [question_id, payload['question'], get_user_id(), product_id, products_version]

        if parents_question_id is not None:

            # TODO: if parents_question_id is not None...

            parent_question_statement = 'select users_user_id from questions where products_product_id = %s and question_id = %s;'
            parent_question_values = [product_id, parents_question_id]

            cur.execute(parent_question_statement, parent_question_values)
            parent_question_rows = cur.fetchone()

            if parent_question_rows[0] is None:
                raise ParentQuestionNotFound(parents_question_id)

            insert_question_values.extend([parents_question_id, parent_question_rows[0]])

        # insert_question_statement = f'insert into questions values (%s, %s, %s, %s, %s, %s{", %s, %s" if parents_question_id is not None else ""});'
        insert_question_statement = f'insert into questions ' \
                                    f'(question_id, question_text, users_user_id, products_product_id, products_version ' \
                                    f'{", questions_question_id, questions_users_user_id" if parents_question_id is not None else ""}) ' \
                                    f'values (%s, %s, %s, %s, %s{", %s, %s" if parents_question_id is not None else ""});'

        cur.execute(insert_question_statement, insert_question_values)

        response = {'status': StatusCodes['success'], 'results': question_id}
        conn.commit()

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



##
# Obtain information about a product with product_id <product_id>
##
# To use it, access through Postman:
##
# GET http://localhost:8080/dbproj/products/7390626
##
@app.route('/dbproj/products/<product_id>', methods=['GET'])
def get_product_info(product_id):
    logger.info('GET /products/<product_id>')
    conn = db_connection()
    cur = conn.cursor()

    try:
        # Get info about the product that have the product_id correspondent to the one given
        statement = 'select name, stock, description, coalesce(avg_rating, -1), price, version,(exists(select comment from ratings where products_product_id = %s and products_version = version))::varchar ' \
                    'from products ' \
                    'where product_id = %s ' \
                    'union ' \
                    'select name, stock, description, avg_rating, price, version, comment ' \
                    'from products, ratings ' \
                    'where product_id = %s and products_product_id = %s and products_version = version'
        values = (product_id,) * 4
        cur.execute(statement, values)
        rows = cur.fetchall()

        if len(rows) == 0:
            raise ProductNotFound(product_id)

        rating = rows[0][3] if rows[0][3] != -1 else 'Product not rated yet'

        comments = [i[6] for i in rows if i[6] not in ['true', 'false']]
        if len(comments) == 0:
            comments = "Product without comments because it wasn't rated yet"

        prices = [f"{i[5]} - {i[4]}" for i in rows if
                  i[6] in ['true', 'false']]  # Format: product_price_version - product_price_associated_to_the_version
        content = {'name': rows[0][0], 'stock': rows[0][1], 'description': rows[0][2], 'prices': prices,
                   'rating': rating, 'comments': comments}

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

    statement = 'select  to_char(order_date, \'MM-YYYY\') as month, sum(price_total), count(id) ' \
                'from orders ' \
                'where order_date > (CURRENT_DATE - interval \'1 year\') ' \
                'group by month;'

    try:
        cur.execute(statement)
        rows = cur.fetchall()

        sale_stats = [{'month': r[0], 'total_value': r[1], 'orders': r[2]} for r in rows]
        response = sale_stats

        # print(sale_stats)  # debug

        response = {'status': StatusCodes['success'], 'results': sale_stats}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


# TODO: create new coupons campaign


# TODO: subscribe to coupons campaigns


# TODO: obtain coupons campaign statistics


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
