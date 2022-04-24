-- TRUNCATE admins, buyers, campaigns, computers, coupons, notifications, orders, product_quantities, products, questions, ratings, sellers, sellers_orders, smartphones, televisions, users;

INSERT INTO users VALUES (0, 'admin', 'dbproj');
INSERT INTO admins VALUES (0);

INSERT INTO users VALUES (1, 'Worten', 'wortensempre');
INSERT INTO sellers VALUES (503630330, 'Avenida D.João 1 Nº 270', 1);

INSERT INTO users VALUES (2, 'gui', 'tcsw');
INSERT INTO buyers VALUES (123456789, 'Praceta da Rua', 2);

INSERT INTO products (product_id, version, name, price, stock, sellers_users_user_id) VALUES (7559297, '2022-04-23 23:33:00', 'Portátil Gaming Lenovo Legion 5', 1199.0, 10, 1);
INSERT INTO computers VALUES (15.6, 'AMD Ryzen 5 5600H', 'NVIDIA GeForce RTX 3060', '512 GB SSD', 120, 7559297, '2022-04-23 23:33:00');

INSERT INTO products (product_id, version, name, price, stock, sellers_users_user_id) VALUES (7423493, '2022-04-24 19:40:00', 'SAMSUNG Galaxy A22 5G', 209.99, 23, 1);
INSERT INTO smartphones VALUES (6.6, 'Android', '128 GB', 'Preto', 7423493, '2022-04-24 19:40:00');

INSERT INTO products (product_id, version, name, price, stock, sellers_users_user_id) VALUES (7390626, '2022-04-24 19:40:00', 'Smart TV Sony 4K', 849.99, 5, 1);
INSERT INTO televisions VALUES (55, 'LCD-LED', '4K Ultra HD', true, 'G', 7390626, '2022-04-24 19:40:00');
