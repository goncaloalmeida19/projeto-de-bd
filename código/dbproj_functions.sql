CREATE EXTENSION IF NOT EXISTS pgcrypto;

create or replace function gettype(product_id int) returns varchar
    language plpgsql
as
$$
declare
    type varchar(20);
begin
    if not exists(select p.product_id from products as p where sellers_users_user_id = seller_id and p.product_id = input_prod_id) then
        type = 'invalid';
    elsif exists(select products_product_id from smartphones where products_product_id = input_prod_id) then
        type = 'smartphones';
    elsif exists(select products_product_id from televisions where products_product_id = input_prod_id) then
        type = 'televisions';
    elsif exists(select products_product_id from computers where products_product_id = input_prod_id) then
        type = 'computers';
    else
        type = 'invalid';
    end if;
    return type;
end;
$$;


create or replace function q_notif() returns trigger
    language plpgsql
as
$$
declare
    parent_user_id questions.questions_users_user_id%type;
    notif_id       notifications.notification_id%type;
    user_id        sellers.users_user_id%type;
begin
    parent_user_id := new.questions_users_user_id;
    select sellers_users_user_id into user_id from products where product_id = new.products_product_id;
    select max(notification_id) into notif_id from notifications where users_user_id = user_id;

    if notif_id is NULL then
        notif_id := 0;
    else
        notif_id := notif_id + 1;
    end if;

    insert into notifications
    values (notif_id, user_id,
            CONCAT('New comment nº', new.question_id, ' regarding your product nº ', new.products_product_id, ': ''',
                   new.question_text, ''''));

    if parent_user_id is not NULL then
        select max(notification_id) into notif_id from notifications where users_user_id = user_id;
        if notif_id is NULL then
            notif_id := 0;
        else
            notif_id := notif_id + 1;
        end if;
        insert into notifications
        values (notif_id, parent_user_id,
                CONCAT('New reply nº', new.question_id, ' to your comment nº', new.questions_question_id,
                       ' on product nº ', new.products_product_id, ': ''', new.question_text, ''''));
    end if;

    return new;
end;
$$;


create or replace function sale_notif() returns trigger
    language plpgsql
as
$$
declare
    notif_id       notifications.notification_id%type;
    total       orders.price_total%type;
    buyer_id    orders.buyers_users_user_id%type;
    campaign_id orders.coupons_campaigns_campaign_id%type;
    sellers cursor for
        select distinct sellers_users_user_id
		from product_quantities as pq, products as p
		where pq.orders_id = new.id and pq.products_product_id = p.product_id;
begin
    total := new.price_total;
    buyer_id := new.buyers_users_user_id;
    campaign_id := new.coupons_campaigns_campaign_id;

    for line in sellers
	loop
        select max(notification_id) into notif_id from notifications where users_user_id = line.sellers_users_user_id;
        if notif_id is NULL then
            notif_id := 0;
        else
            notif_id := notif_id + 1;
        end if;

        insert into sellers_orders
        values (line.sellers_users_user_id, new.id);

		insert into notifications
        values (notif_id, line.sellers_users_user_id,
            CONCAT('New order nº', new.id, ' including your products'));
	end loop;


    select max(notification_id) into notif_id from notifications where users_user_id = buyer_id;
    if notif_id is NULL then
        notif_id := 0;
    else
        notif_id := notif_id + 1;
    end if;
    insert into notifications
    values (notif_id, buyer_id,
            CONCAT('Your order nº', new.id, ' for a total of ', new.price_total, ' has been confirmed'));

    return new;
end;
$$;


create or replace function rating_notif() returns trigger
    language plpgsql
as
$$
declare
    notif_id       notifications.notification_id%type;
    seller_id sellers.users_user_id%type;
begin
    select sellers_users_user_id into seller_id from products where product_id = new.products_product_id;

    select max(notification_id) into notif_id from notifications where users_user_id = seller_id;
    if notif_id is NULL then
        notif_id := 0;
    else
        notif_id := notif_id + 1;
    end if;

    insert into notifications
    values (notif_id, seller_id,
            CONCAT('Your product nº', new.products_product_id, ' (version ', new.products_version,
                ') has been rated a ', new.rating,
                ' with the comment ''', new.comment, ''''));

    return new;
end;
$$;


drop trigger if exists q_notif_trig on questions;
create trigger q_notif_trig
    before insert
    on questions
    for each row
execute function q_notif();


drop trigger if exists sale_notif_trig on orders;
create trigger sale_notif_trig
    before update
    on orders
    for each row
execute function sale_notif();


drop trigger if exists rating_notif_trig on products;
create trigger rating_notif_trig
    before insert
    on ratings
    for each row
execute function rating_notif();