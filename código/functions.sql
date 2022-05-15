create or replace function gettype(product_id int) returns varchar
    language plpgsql
as
$$
declare
    type varchar(20);
begin
    if exists(select products_product_id from smartphones where products_product_id = product_id) then
        type = 'smartphones';
    elsif exists(select products_product_id from televisions where products_product_id = product_id) then
        type = 'televisions';
    elsif exists(select products_product_id from computers where products_product_id = product_id) then
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

drop trigger if exists q_notif_trig on questions;
create trigger q_notif_trig
    before insert
    on questions
    for each row
execute function q_notif();
