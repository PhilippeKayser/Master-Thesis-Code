

CREATE or replace VIEW vw_payments
AS
--select SYS.STANDARD.TO_CHAR(pf_guid) as pf_guid, year, halfyear, quarter, month, monthname, week, weekday, weekdayname, day, dim_sessions_id, session_status, emp_name, cust_name, payment_method, discount, bonus_points, price_paid
--from (
    select
        SYS.STANDARD.TO_CHAR(pf.pf_guid) as pf_guid,
        dd.year,
        dd.halfyear,
        dd.quarter,
        dd.month,
        dd.monthname,
        dd.week,
        dd.weekday,
        dd.weekdayname,
        dd.day,
        ds.dim_sessions_id,
        ds.session_status,
        pf.price_paid,
        pf.bonus_points,
        pf.discount,
        dpm.payment_method,
        de.last_name || ' ' || de.first_name as EMP_NAME,
        dc.last_name || ' ' || dc.first_name as CUST_NAME
    from
        payment_fact pf
    join dim_date dd on dd.dim_date_id = pf.dim_date_id
    join dim_sessions ds on ds.dim_sessions_id = pf.dim_sessions_id
    join dim_payment_methods dpm on dpm.dim_payment_methods_id = pf.dim_payment_methods_id
    join dim_employees de on de.dim_employees_id = pf.dim_employees_id
    join dim_customers dc on dc.dim_customers_id = pf.dim_customers_id
    order by dd.year, dd.halfyear, dd.quarter, dd.month, dd.day, ds.session_status, dpm.payment_method
--)
/

create or replace view vw_detailed_item_sales
as
--select SYS.STANDARD.TO_CHAR(isf_guid) as isf_guid, SYS.STANDARD.TO_CHAR(disf_guid) as disf_guid, custom_original_item_id, custom_item_name, original_selg_id, selg_name, original_sel_id, sel_name, original_exitem_id, extra_item_name
--from (
    select
        SYS.STANDARD.TO_CHAR(isf.isf_guid) as isf_guid,
        SYS.STANDARD.TO_CHAR(disf.disf_guid) as disf_guid,
        pci.original_item_id as custom_original_item_id,
        pci.name as custom_item_name,
        dsel.original_selg_id,
        dsel.selg_name,
        dsel.original_sel_id,
        dsel.sel_name,
        dext.original_exitem_id,
        dext.name as extra_item_name
    from
        item_sales_fact isf
    left join detailed_item_sales_fact disf on disf.isf_guid = isf.isf_guid
    join prod_custom_items pci on pci.prod_custom_items_id = disf.prod_custom_items_id
    join dim_selections dsel on dsel.dim_selections_id = disf.dim_selections_id
    join dim_extra_items dext on dext.dim_extra_items_id = disf.dim_extra_items_id
    join dim_date dd on dd.dim_date_id = isf.dim_date_id
    join dim_time dt on dt.dim_time_id = isf.dim_time_id
    where   to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between dsel.from_date and dsel.end_date
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between dext.from_date and dext.end_date
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pci.from_date and pci.end_date
--)
/

create or replace view vw_item_sales
as
select SYS.STANDARD.TO_CHAR(isf_guid) as isf_guid, year, halfyear, quarter, month, day, week, weekday, weekdayname, hour, minute, cust_name, emp_name, original_menu_id, dim_sessions_id, session_status, menu_name, original_cat_id, cat_name, original_item_id, item_name, quantity_sold, amount
from (
    select
        isf.isf_guid,
        dd.year,
        dd.halfyear,
        dd.quarter,
        dd.month,
        dd.monthname,
        dd.week,
        dd.weekday,
        dd.weekdayname,
        dd.day,
        dt.hour,
        dt.minute,
        dc.last_name || ' ' || dc.first_name as cust_name,
        de.last_name || ' ' || de.first_name as emp_name,
        ds.dim_sessions_id,
        ds.session_status,
        pmh.menu_name,
        pm.original_menu_id,
        pch.name as cat_name,
        pc.original_cat_id,
        pih.name as item_name,
        pi.original_item_id,
        isf.quantity_sold,
        isf.quantity_sold * isf.price as amount
    from
        item_sales_fact isf
    join dim_date dd on dd.dim_date_id = isf.dim_date_id
    join dim_time dt on dt.dim_time_id = isf.dim_time_id
    join prod_items pi on pi.prod_items_id = isf.prod_items_id
    join prod_items_history pih on pih.prod_items_id = pi.prod_items_id
    join prod_categories pc on pc.prod_cat_id = pi.prod_cat_id
    join prod_categories_history pch on pch.prod_cat_id = pc.prod_cat_id
    join prod_menus pm on pm.prod_menus_id = pc.prod_menus_id
    join prod_menus_history pmh on pmh.prod_menus_id = pm.prod_menus_id
    join dim_customers dc on dc.dim_customers_id = isf.dim_customers_id
    join dim_employees de on de.dim_employees_id = isf.dim_employees_id
    join dim_sessions ds on ds.dim_sessions_id = isf.dim_sessions_id
    where pm.original_mt_id = 1 
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pmh.from_date and pmh.end_date
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pch.from_date and pch.end_date
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pih.from_date and pih.end_date      
    UNION    
    select
        isf.isf_guid,
        dd.year,
        dd.halfyear,
        dd.quarter,
        dd.month,
        dd.monthname,
        dd.week,
        dd.weekday,
        dd.weekdayname,
        dd.day,
        dt.hour,
        dt.minute,
        dc.last_name || ' ' || dc.first_name as cust_name,
        de.last_name || ' ' || de.first_name as emp_name,
        ds.dim_sessions_id,
        ds.session_status,
        pmh.menu_name,
        pm.original_menu_id,
        pch.name as cat_name,
        pc.original_cat_id,
        pih.name as item_name,
        pi.original_item_id,
        isf.quantity_sold,
        isf.quantity_sold * isf.price as amount
    from
        item_sales_fact isf
    join dim_date dd on dd.dim_date_id = isf.dim_date_id
    join dim_time dt on dt.dim_time_id = isf.dim_time_id
    join prod_items pi on pi.prod_items_id = isf.prod_items_id
    join prod_items_history pih on pih.prod_items_id = pi.prod_items_id
    join prod_categories pc on pc.prod_cat_id = pi.prod_cat_id
    join prod_categories_history pch on pch.prod_cat_id = pc.prod_cat_id
    join prod_menus pm on pm.prod_menus_id = pi.prod_menus_id
    join prod_menus_history pmh on pmh.prod_menus_id = pm.prod_menus_id
    join dim_customers dc on dc.dim_customers_id = isf.dim_customers_id
    join dim_employees de on de.dim_employees_id = isf.dim_employees_id
    join dim_sessions ds on ds.dim_sessions_id = isf.dim_sessions_id
    where pm.original_mt_id = 2 
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pmh.from_date and pmh.end_date
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pch.from_date and pch.end_date
        and to_date(dd.year || '.' || lpad(dd.month,2,'0') || '.' || lpad(dd.day,2,'0') || ' ' || lpad(dt.hour,2,'0') || ':' || lpad(dt.minute,2,'0'), 'YYYY.MM.DD HH24:MI') between pih.from_date and pih.end_date
)
/

commit;