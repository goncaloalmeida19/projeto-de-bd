create or replace function gettype(product_id int) returns varchar
language plpgsql
as $$
declare type varchar(20);
begin
if exists(select products_product_id from smartphones where products_product_id = product_id) then
	type = 'smartphones';
elsif exists(select products_product_id from televisions where products_product_id = product_id) then
	type = 'televisions';
elsif exists(select products_product_id from computers where products_product_id = product_id) then
	type = 'computers';
else type = 'invalid';
end if;
return type;
end;
$$;

create or replace function q_notif() returns trigger
language plpgsql
as $$
declare
	notif_id notifications.notification_id%type;
	seller_id sellers.users_user_id%type;
begin
    select sellers_users_user_id into seller_id from products where product_id = new.products_product_id;
    select max(notification_id) into notif_id from notifications where users_user_id = seller_id;

	if notif_id is NULL then notif_id := 0; end if;

    insert into notifications
        values(notif_id,
               CONCAT('New question about your product nº', new.products_product_id),
               seller_id
               );
    return new;
end;
$$;

drop trigger if exists q_notif_trig on questions;
create trigger q_notif_trig
before insert on questions for each row execute function q_notif();
