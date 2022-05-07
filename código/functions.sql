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
