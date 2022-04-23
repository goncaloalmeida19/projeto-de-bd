INSERT INTO users VALUES (0, 'admin', 'dbproj');
INSERT INTO admins VALUES (0);

INSERT INTO users VALUES (1, 'Worten', 'wortensempre');
INSERT INTO sellers VALUES (503630330, 'Avenida D.João 1 Nº 270', 1);

INSERT INTO users VALUES (2, 'gui', 'tcsw');
INSERT INTO buyers VALUES (123456789, 'Praceta da Rua', 2);

INSERT INTO products (product_id, version, name, price, stock, sellers_users_user_id) VALUES (7559297, '2022-04-23 23:33:00', 'Portátil Gaming Lenovo Legion 5', 1199.0, 10, 1);
INSERT INTO computers VALUES (15.6, 'AMD Ryzen 5 5600H', 'NVIDIA GeForce RTX 3060', '512 GB SSD', 120, 7559297, '2022-04-23 23:33:00');