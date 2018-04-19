prompt Creating Views
create or replace view vw_employees
as
select e.emp_id, p.last_name, p.first_name, p.middle_names, p.gender, p.email, p.birthdate, e.manager_id, e.job_id, j.job_title, e.social_sec_num, e.warnings, e.salary
from persons p
join employee_details e on e.emp_id = p.pers_id
join jobs j on j.job_id = e.job_id
where ptype = 'E';
/

create or replace view vw_customers
as
select pers_id, last_name, first_name, middle_names, gender, email, birthdate
from persons
where ptype = 'C';
/

create or replace view vw_menu_structure
as
select mt.mt_id, mt.name as mt_name, m.menu_id, m.name as menu_name, m.description as menu_description, nc.cat_id, nc.name as cat_name, nc.description as cat_description, nc.weight as cat_weight
from menu_types mt
join menus m on m.mt_id = mt.mt_id
join nested_categories nc on nc.menu_id = m.menu_id
order by mt.mt_id, m.menu_id, nc.lft;
/

create or replace view vw_selections
as
select sg.selg_id, sg.name as sg_name, sel.sel_id, sel.name as sel_name, sel.description as sel_description
from selection_groups sg
join selgs_sels sgs on sgs.selg_id = sg.selg_id
join selections sel on sel.sel_id = sgs.sel_id
order by sg.selg_id, sel.sel_id;
/

create or replace view vw_items
as
select mt.mt_id, mt.name as mt_name, m.menu_id, m.name as menu_name, nc.cat_id, nc.name as cat_name, it.item_id, it.name as item_name, it.subtitle as item_subtitle, it.description as item_description, to_char(ip.price,'9990.00') as item_price, ip.price_name as item_price_name
from items it
join item_prices ip on ip.item_id = it.item_id
join nested_categories nc on nc.cat_id = it.cat_id
join menus m on m.menu_id = nc.menu_id
join menu_types mt on mt.mt_id = m.mt_id
where mt.weighted = 0
order by it.item_id, ip.price;
/

create or replace view vw_weighted_items
as
select m.menu_id, m.name as menu_name, nc.cat_id, nc.name as cat_name, nc.weight, it.item_id, it.name as item_name, it.subtitle as item_subtitle, it.description as item_description
from items it
join nested_categories nc on nc.cat_id = it.cat_id
join menus m on m.menu_id = nc.menu_id
join menu_types mt on mt.mt_id = m.mt_id
where mt.weighted = 1
order by m.menu_id, nc.weight, nc.lft, it.item_id;
/

create or replace view vw_weighted_menus_prices
as
select m.menu_id, m.name as menu_name, wmp.weight, it.item_id, it.name as item_name, it.subtitle as item_subtitle, it.description as item_description, to_char(ip.price,'9990.00') as item_price, ip.price_name as item_price_name
from items it
join item_prices ip on ip.item_id = it.item_id
join w_menus_prices wmp on wmp.item_id = it.item_id
join menus m on m.menu_id = wmp.menu_id
order by m.menu_id, wmp.weight, ip.price;
/