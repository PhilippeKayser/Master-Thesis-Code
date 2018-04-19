CREATE OR REPLACE VIEW vw_prod_items AS
SELECT p.prod_items_id, ph.from_date, ph.end_date, p.original_item_id, ph.name, ph.subtitle, ph.description, ph.price, ph.price_name, ph.allergenes, pc.original_cat_id, pm.original_menu_id 
FROM prod_items p 
JOIN prod_items_history ph on ph.prod_items_id = p.prod_items_id
JOIN prod_categories pc on pc.prod_cat_id = ph.prod_cat_id
JOIN prod_menus pm on pm.prod_menus_id = ph.prod_menus_id
WHERE SYSTIMESTAMP between ph.from_date and ph.end_date;
/

CREATE OR REPLACE VIEW vw_prod_categories AS
SELECT pc.prod_cat_id, pch.from_date, pch.end_date, pc.original_cat_id, pch.name, pch.description, pch.lft, pch.rgt, pm.original_menu_id
FROM prod_categories pc
JOIN prod_categories_history pch ON pch.prod_cat_id = pc.prod_cat_id
JOIN prod_menus pm ON pm.prod_menus_id = pch.prod_menus_id
WHERE SYSTIMESTAMP between pch.from_date and pch.end_date;
/

CREATE OR REPLACE VIEW vw_prod_menus AS
SELECT pm.prod_menus_id, pmh.from_date, pmh.end_date, pm.original_menu_id, pmh.menu_name, pmh.menu_description, pm.original_mt_id, pmh.mt_name
FROM prod_menus pm
JOIN prod_menus_history pmh on pmh.prod_menus_id = pm.prod_menus_id
WHERE SYSTIMESTAMP between pmh.from_date and pmh.end_date;
/

CREATE OR REPLACE VIEW vw_prod_custom_items AS
SELECT pci.prod_custom_items_id, pci.from_date, pci.end_date, pci.original_item_id, pci.name, pci.subtitle, pci.description, pci.allergenes, pc.original_cat_id, pm.original_menu_id
FROM prod_custom_items pci
JOIN prod_categories pc on pc.prod_cat_id = pci.prod_cat_id
JOIN prod_menus pm on pm.prod_menus_id = pci.prod_menus_id
WHERE SYSTIMESTAMP between pci.from_date and pci.end_date;
/

CREATE OR REPLACE VIEW vw_dim_extra_items AS
SELECT dim_extra_items_id, from_date, end_date, original_exitem_id, name, price, allergenes
FROM dim_extra_items
WHERE SYSTIMESTAMP between from_date and end_date;
/

CREATE OR REPLACE VIEW vw_dim_selections AS
SELECT dim_selections_id, from_date, end_date, original_selg_id, selg_name, original_sel_id, sel_name, sel_description 
FROM dim_selections
WHERE SYSTIMESTAMP between from_date and end_date;
/

CREATE OR REPLACE VIEW vw_dim_employees AS
SELECT de.dim_employees_id, deh.from_date, deh.end_date, de.original_emp_id, deh.last_name, deh.first_name, deh.middle_names, de.gender, de.birthdate, de.original_job_id, deh.job_title
FROM dim_employees de
JOIN dim_employees_history deh ON deh.dim_employees_id = de.dim_employees_id
WHERE SYSTIMESTAMP between deh.from_date and deh.end_date;
/

CREATE OR REPLACE VIEW vw_dim_customers AS
SELECT dim_customers_id, original_pers_id, last_name, first_name, middle_names, gender, birthdate, email
FROM dim_customers;
/

/*
CREATE OR REPLACE VIEW vw_dim_customers AS
SELECT dc.dim_customers_id, dch.from_date, dch.end_date, dc.original_pers_id, dc.last_name, dc.first_name, dc.middle_names, dc.gender, dc.birthdate, dc.email
FROM dim_customers dc
JOIN dim_customers_history dch ON dch.dim_customers_id = dc.dim_customers_id
WHERE SYSTIMESTAMP between dch.from_date and dch.end_date;
/
*/