\c dbproj

drop function if exists q_notif() cascade;
drop function if exists sale_notif() cascade;
drop function if exists rating_notif() cascade;

drop table if exists admins cascade;
drop table if exists buyers cascade;
drop table if exists campaigns cascade;
drop table if exists computers cascade;
drop table if exists coupons cascade;
drop table if exists notifications cascade;
drop table if exists orders cascade;
drop table if exists product_quantities cascade;
drop table if exists products cascade;
drop table if exists questions cascade;
drop table if exists ratings cascade;
drop table if exists sellers cascade;
drop table if exists sellers_orders cascade;
drop table if exists smartphones cascade;
drop table if exists televisions cascade;
drop table if exists users cascade;

REVOKE ALL ON ALL TABLES IN SCHEMA public FROM projuser;
REVOKE CONNECT ON DATABASE dbproj FROM projuser;

drop user projuser;

\c postgres

drop database dbproj;
