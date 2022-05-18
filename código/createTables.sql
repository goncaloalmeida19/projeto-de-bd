/* Create table products */
CREATE TABLE products (
	product_id		 INTEGER,
	name			 VARCHAR(512) NOT NULL,
	price		 DOUBLE PRECISION NOT NULL,
	stock		 INTEGER NOT NULL,
	description		 VARCHAR(512) NOT NULL,
	avg_rating FLOAT,
	sellers_users_user_id INTEGER NOT NULL,
	version		 TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
	PRIMARY KEY(product_id,version)
);

/* Create table computers */
CREATE TABLE computers (
	screen_size	 FLOAT(8) NOT NULL,
	cpu		 VARCHAR(512) NOT NULL,
	gpu		 VARCHAR(512) NOT NULL,
	storage		 VARCHAR(512) NOT NULL,
	refresh_rate	 INTEGER NOT NULL,
	products_product_id INTEGER,
	products_version	 TIMESTAMP,
	PRIMARY KEY(products_product_id,products_version)
);

/* Create table televisions */
CREATE TABLE televisions (
	screen_size	 FLOAT(8) NOT NULL,
	screen_type	 VARCHAR(512) NOT NULL,
	resolution		 VARCHAR(512) NOT NULL,
	smart		 BOOL NOT NULL,
	efficiency		 CHAR(255) NOT NULL,
	products_product_id INTEGER,
	products_version	 TIMESTAMP,
	PRIMARY KEY(products_product_id,products_version)
);

/* Create table smartphones */
CREATE TABLE smartphones (
	screen_size	 FLOAT(8) NOT NULL,
	os			 VARCHAR(512) NOT NULL,
	storage		 VARCHAR(512) NOT NULL,
	color		 VARCHAR(512) NOT NULL,
	products_product_id INTEGER,
	products_version	 TIMESTAMP,
	PRIMARY KEY(products_product_id,products_version)
);

/* Create table users */
CREATE TABLE users (
	user_id	 INTEGER,
	username VARCHAR(512) UNIQUE NOT NULL,
	password VARCHAR(512) NOT NULL,
	email VARCHAR(512) NOT NULL,
	PRIMARY KEY(user_id)
);

/* Create table admins */
CREATE TABLE admins (
	users_user_id INTEGER,
	PRIMARY KEY(users_user_id)
);

/* Create table sellers */
CREATE TABLE sellers (
	users_user_id INTEGER,
	nif		 INTEGER NOT NULL,
	shipping_addr VARCHAR(512) NOT NULL,
	PRIMARY KEY(users_user_id)
);

/* Create table buyers */
CREATE TABLE buyers (
	users_user_id INTEGER,
	nif		 INTEGER NOT NULL,
	home_addr	 VARCHAR(512) NOT NULL,
	PRIMARY KEY(users_user_id)
);

/* Create table orders */
CREATE TABLE orders (
	id				 INTEGER,
	order_date			 DATE NOT NULL,
	price_total			 DOUBLE PRECISION NOT NULL DEFAULT 0,
	coupons_coupon_id		 INTEGER,
	coupons_campaigns_campaign_id INTEGER,
	buyers_users_user_id		 INTEGER NOT NULL,
	PRIMARY KEY(id)
);

/* Create table ratings */
CREATE TABLE ratings (
	comment		 VARCHAR(512),
	rating	 SMALLINT NOT NULL,
	orders_id		 INTEGER,
	products_product_id	 INTEGER,
	products_version	 TIMESTAMP,
	buyers_users_user_id INTEGER NOT NULL,
	PRIMARY KEY(orders_id,products_product_id,products_version)
);

/* Create table questions */
CREATE TABLE questions (
	question_id			 INTEGER,
	question_text		 VARCHAR(512) NOT NULL,
	users_user_id		 INTEGER NOT NULL,
	products_product_id		 INTEGER NOT NULL,
	products_version		 TIMESTAMP NOT NULL,
	questions_question_id	 INTEGER,
	questions_users_user_id	 INTEGER,
	PRIMARY KEY(question_id,products_product_id)
);

/* Create table campaigns */
CREATE TABLE campaigns (
	campaign_id		 INTEGER,
	description		 VARCHAR(512) NOT NULL,
	date_start		 DATE NOT NULL,
	date_end		 DATE NOT NULL,
	coupons          INTEGER NOT NULL,
	discount		 DOUBLE PRECISION NOT NULL,
	admins_users_user_id INTEGER NOT NULL,
	PRIMARY KEY(campaign_id)
);

/* Create table coupons */
CREATE TABLE coupons (
	coupon_id		 INTEGER,
	used			 BOOL NOT NULL,
	discount_applied	 DOUBLE PRECISION,
	expiration_date DATE NOT NULL,
	campaigns_campaign_id INTEGER,
	buyers_users_user_id	 INTEGER NOT NULL,
	orders_id		 INTEGER,
	PRIMARY KEY(coupon_id,campaigns_campaign_id)
);

/* Create table notifications */
CREATE TABLE notifications (
	notification_id INTEGER,
	users_user_id	 INTEGER NOT NULL,
	content	 VARCHAR(512) NOT NULL,
	time		 TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
	PRIMARY KEY(notification_id, users_user_id)
);

/* Create table product_quantities */
CREATE TABLE product_quantities (
	quantity		 INTEGER NOT NULL,
	orders_id		 INTEGER,
	products_product_id INTEGER,
	products_version	 TIMESTAMP,
	PRIMARY KEY(orders_id,products_product_id,products_version)
);

/* Create table sellers_orders */
CREATE TABLE sellers_orders (
	sellers_users_user_id INTEGER,
	orders_id		 INTEGER,
	PRIMARY KEY(sellers_users_user_id,orders_id)
);

ALTER TABLE products ADD CONSTRAINT products_fk1 FOREIGN KEY (sellers_users_user_id) REFERENCES sellers(users_user_id);
ALTER TABLE computers ADD CONSTRAINT computers_fk1 FOREIGN KEY (products_product_id, products_version) REFERENCES products(product_id, version);
ALTER TABLE televisions ADD CONSTRAINT televisions_fk1 FOREIGN KEY (products_product_id, products_version) REFERENCES products(product_id, version);
ALTER TABLE smartphones ADD CONSTRAINT smartphones_fk1 FOREIGN KEY (products_product_id, products_version) REFERENCES products(product_id, version);
ALTER TABLE admins ADD CONSTRAINT admins_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE sellers ADD CONSTRAINT sellers_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE buyers ADD CONSTRAINT buyers_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE orders ADD CONSTRAINT orders_fk1 FOREIGN KEY (coupons_coupon_id, coupons_campaigns_campaign_id) REFERENCES coupons(coupon_id, campaigns_campaign_id);
ALTER TABLE orders ADD CONSTRAINT orders_fk3 FOREIGN KEY (buyers_users_user_id) REFERENCES buyers(users_user_id);
ALTER TABLE ratings ADD CONSTRAINT ratings_fk1 FOREIGN KEY (orders_id) REFERENCES orders(id);
ALTER TABLE ratings ADD CONSTRAINT ratings_fk2 FOREIGN KEY (products_product_id, products_version) REFERENCES products(product_id, version);
ALTER TABLE ratings ADD CONSTRAINT ratings_fk4 FOREIGN KEY (buyers_users_user_id) REFERENCES buyers(users_user_id);
ALTER TABLE questions ADD CONSTRAINT questions_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE questions ADD CONSTRAINT questions_fk3 FOREIGN KEY (products_product_id, products_version) REFERENCES products(product_id, version);
ALTER TABLE questions ADD CONSTRAINT questions_fk5 FOREIGN KEY (questions_question_id, products_product_id) REFERENCES questions(question_id, products_product_id);
ALTER TABLE campaigns ADD CONSTRAINT campaigns_fk1 FOREIGN KEY (admins_users_user_id) REFERENCES admins(users_user_id);
ALTER TABLE coupons ADD CONSTRAINT coupons_fk1 FOREIGN KEY (campaigns_campaign_id) REFERENCES campaigns(campaign_id);
ALTER TABLE coupons ADD CONSTRAINT coupons_fk2 FOREIGN KEY (buyers_users_user_id) REFERENCES buyers(users_user_id);
ALTER TABLE coupons ADD CONSTRAINT coupons_fk3 FOREIGN KEY (orders_id) REFERENCES orders(id);
ALTER TABLE notifications ADD CONSTRAINT notifications_fk2 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE product_quantities ADD CONSTRAINT product_quantities_fk1 FOREIGN KEY (orders_id) REFERENCES orders(id);
ALTER TABLE product_quantities ADD CONSTRAINT product_quantities_fk2 FOREIGN KEY (products_product_id, products_version) REFERENCES products(product_id, version);
ALTER TABLE sellers_orders ADD CONSTRAINT sellers_orders_fk1 FOREIGN KEY (sellers_users_user_id) REFERENCES sellers(users_user_id);
ALTER TABLE sellers_orders ADD CONSTRAINT sellers_orders_fk2 FOREIGN KEY (orders_id) REFERENCES orders(id);

