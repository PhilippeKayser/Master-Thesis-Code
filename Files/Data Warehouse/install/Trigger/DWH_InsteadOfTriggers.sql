-- INSTEAD OF TRIGGERS 

-- DIM_SELECTIONS
--DONT NEED INSERT AS IT IS JUST A REGULAR INSERT
CREATE OR REPLACE TRIGGER trg_dim_selections_update
INSTEAD OF UPDATE
ON vw_dim_selections
DECLARE
	v_new_dsid number;
    v_old_dsid number;
BEGIN
	IF :new.original_selg_id != :old.original_selg_id OR :new.original_sel_id != :old.original_sel_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Keys in dim_selections do not match up.');
	END IF;

	
    --INSERT EARLIER DATA
    IF :NEW.from_date < :old.from_date THEN
        --Update olf data set    
        UPDATE dim_selections
        SET end_date = :new.from_date
        WHERE original_selg_id = :new.original_selg_id AND original_sel_id = :new.original_sel_id AND end_date between :new.from_date and :old.from_date
        RETURNING dim_selections_id INTO v_old_dsid;

    	--INSERT A NEW ROW WITH THE NEW DATA
	    INSERT INTO dim_selections(from_date, end_date, original_selg_id, selg_name, original_sel_id, sel_name, sel_description)
	    VALUES (:new.from_date, :old.from_date, :NEW.original_selg_id, :NEW.selg_name, :NEW.original_sel_id, :NEW.sel_name, :NEW.sel_description)
	    RETURNING dim_selections_id INTO v_new_dsid;

        dbms_output.put_line(v_old_dsid);
        dbms_output.put_line(v_new_dsid);
        
	    --Update elements in detailed item_sales_fact
	    UPDATE detailed_item_sales_fact
	    SET dim_selections_id = v_new_dsid
	    WHERE dim_selections_id = v_old_dsid AND
            EXISTS (
                SELECT disf.disf_guid
                FROM detailed_item_sales_fact disf
                JOIN dim_date dd on  dd.dim_date_id = disf.dim_date_id
                JOIN dim_time dt on dt.dim_time_id = disf.dim_time_id
                WHERE  to_date(dd.year || '.' ||LPAD(dd.month,2,'0') ||'.'|| LPAD(dd.day,2,'0')||' '||dt.hour||':'||dt.minute,'YYYY.MM.DD HH24:MI') between to_date(:new.from_date) and to_date(:old.from_date)
            );
    ELSIF :new.from_date > :old.from_date THEN
    
        --UPDATE THE END DATE OF THE CURRENT ELEMENT
        UPDATE dim_selections 
        SET end_date = :new.from_date 
        WHERE dim_selections_id = :new.dim_selections_id;

    
	    --INSERT A NEW ROW WITH THE NEW DATA
	    INSERT INTO dim_selections(from_date, original_selg_id, selg_name, original_sel_id, sel_name, sel_description)
	    VALUES (:new.from_date, :NEW.original_selg_id, :NEW.selg_name, :NEW.original_sel_id, :NEW.sel_name, :NEW.sel_description);
    ELSE
        RAISE_APPLICATION_ERROR(-20011,'ERROR');
    END IF;
END trg_dim_selections_update;
/

CREATE OR REPLACE TRIGGER trg_dim_selections_delete
INSTEAD OF DELETE
ON vw_dim_selections
BEGIN
    UPDATE dim_selections 
    SET end_date = SYSDATE 
    WHERE dim_selections_id = :old.dim_selections_id;
END trg_dim_selections_delete;
/


--PROD_ITEMS
CREATE OR REPLACE TRIGGER trg_prod_items_insert
INSTEAD OF INSERT
ON vw_prod_items
DECLARE
	pi_id number;

	pc_id number;
	pm_id number;
BEGIN
	

	SELECT pc.prod_cat_id, pm.prod_menus_id INTO pc_id, pm_id FROM prod_categories pc, prod_menus pm WHERE pc.original_cat_id = :new.original_cat_id AND pm.original_menu_id = :new.original_menu_id; 

	--Insert new Element into prod_items
	INSERT INTO prod_items(original_item_id, name, subtitle, description, price, price_name, allergenes, prod_cat_id, prod_menus_id)
    VALUES (:new.original_item_id, :new.name, :new.subtitle, :new.description, :new.price, :new.price_name, :new.allergenes, pc_id, pm_id)
    RETURNING prod_items_id INTO pi_id;

    --Insert new Value into history
    INSERT INTO prod_items_history(prod_items_id, name, subtitle, description, price, price_name, allergenes, prod_cat_id, prod_menus_id)
	VALUES (pi_id, :new.name, :new.subtitle, :new.description, :new.price, :new.price_name, :new.allergenes, pc_id, pm_id);
END trg_prod_items_insert;
/

create or replace TRIGGER trg_prod_items_update
INSTEAD OF UPDATE
ON vw_prod_items
DECLARE
    pi_id number;

    pc_id number;
	pm_id number;

	v_price_changed boolean := false;
	v_info_changed boolean := false;
BEGIN

	IF :old.original_item_id != :new.original_item_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Key in prod_items does not match.');
	END IF;

	--Update end date of current element


	if :old.price != :new.price or :old.price_name != :new.price_name then
		UPDATE prod_items
		SET price = :new.price, price_name = :new.price_name
		WHERE prod_items_id = :new.prod_items_id and price = :old.price;

		v_price_changed := true;
	end if;

	if :new.name != :old.name or :new.subtitle != :old.subtitle or :new.description != :old.description or :new.allergenes != :old.allergenes or :new.original_cat_id != :old.original_cat_id or :new.original_menu_id != :old.original_menu_id then
		UPDATE prod_items
		SET name = :new.name, subtitle = :new.subtitle, description = :new.description, allergenes = :new.allergenes
		WHERE original_item_id = :new.original_item_id;

		v_info_changed := true;
	end if;


	if v_price_changed and not v_info_changed then

		UPDATE prod_items_history
		SET end_date = :new.from_date
		WHERE prod_items_id = :new.prod_items_id and price =:old.price
		AND :new.from_date between from_date and end_date;

		SELECT pc.prod_cat_id, pm.prod_menus_id INTO pc_id, pm_id FROM prod_categories pc, prod_menus pm WHERE pc.original_cat_id = :new.original_cat_id AND pm.original_menu_id = :new.original_menu_id; 

		INSERT INTO prod_items_history(from_date, prod_items_id, name, subtitle, description, price, price_name, allergenes, prod_cat_id, prod_menus_id)
		VALUES (:new.from_date, :new.prod_items_id, :new.name, :new.subtitle, :new.description, :new.price, :new.price_name, :new.allergenes, pc_id, pm_id);

	elsif v_info_changed then
		UPDATE prod_items_history
		SET end_date = :new.from_date
		WHERE prod_items_id IN (
            SELECT prod_items_id FROM prod_items WHERE original_item_id = :new.original_item_id
        )
		AND :new.from_date between from_date and end_date;

		--SELECT pc.prod_cat_id, pm.prod_menus_id INTO pc_id, pm_id FROM prod_categories pc, prod_menus pm WHERE pc.original_cat_id = :new.original_cat_id AND pm.original_menu_id = :new.original_menu_id; 

		INSERT INTO prod_items_history(from_date, prod_items_id, name, subtitle, description, price, price_name, allergenes, prod_cat_id, prod_menus_id)
		SELECT :new.from_date, prod_items_id, name, subtitle, description, price, price_name, allergenes, prod_cat_id, prod_menus_id
		FROM prod_items
		WHERE original_item_id = :new.original_item_id;
	end if;


END trg_prod_items_update;
/

CREATE OR REPLACE TRIGGER trg_prod_items_delete
INSTEAD OF DELETE
ON vw_prod_items
BEGIN
	UPDATE prod_items_history
    SET end_date = SYSDATE 
    WHERE prod_items_id = :old.prod_items_id
    AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;
END trg_prod_items_delete;
/
--PROD_CATEGORIES

create or replace TRIGGER trg_prod_categories_insert
INSTEAD OF INSERT
ON vw_prod_categories
DECLARE
	pc_id number;
	pm_id number;
BEGIN

	SELECT pm.prod_menus_id INTO pm_id FROM prod_menus pm LEFT JOIN prod_menus_history pmh on pmh.prod_menus_id = pm.prod_menus_id WHERE pm.original_menu_id = :new.original_menu_id AND SYSTIMESTAMP BETWEEN pmh.from_date AND pmh.end_date;

	INSERT INTO prod_categories(original_cat_id, name, description, lft, rgt, prod_menus_id)
	VALUES (:new.original_cat_id, :new.name, :new.description, :new.lft, :new.rgt, pm_id)
	RETURNING prod_cat_id INTO pc_id;

	INSERT INTO prod_categories_history(prod_cat_id, name, description, lft, rgt, prod_menus_id)
	VALUES (pc_id, :new.name, :new.description, :new.lft, :new.rgt, pm_id);
END trg_prod_categories_insert;
/

CREATE OR REPLACE TRIGGER trg_prod_categories_update
INSTEAD OF UPDATE
ON vw_prod_categories
DECLARE
	pm_id number;
BEGIN

	IF :old.original_cat_id != :new.original_cat_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Keys in prod_categories do not match up');
	END IF;

	SELECT pm.prod_menus_id INTO pm_id FROM prod_menus pm LEFT JOIN prod_menus_history pmh on pmh.prod_menus_id = pm.prod_menus_id WHERE pm.original_menu_id = :new.original_menu_id AND SYSTIMESTAMP BETWEEN pmh.from_date AND pmh.end_date;

	UPDATE prod_categories_history
	SET end_date = :new.from_date
	WHERE prod_cat_id = :old.prod_cat_id
	AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;

	UPDATE prod_categories
	SET name = :new.name, description = :new.description, lft = :new.lft, rgt = :new.rgt, prod_menus_id = pm_id
	WHERE prod_cat_id = :new.prod_cat_id;

	INSERT INTO prod_categories_history(from_date, prod_cat_id, name, description, lft, rgt, prod_menus_id)
	VALUES (:new.from_date, :new.prod_cat_id, :new.name, :new.description, :new.lft, :new.rgt, pm_id);

END trg_prod_categories_update;
/

CREATE OR REPLACE TRIGGER trg_prod_categories_delete
INSTEAD OF DELETE
ON vw_prod_categories
BEGIN
	UPDATE prod_categories_history
	SET end_date = SYSDATE
	WHERE prod_cat_id = :old.prod_cat_id
	AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;
END trg_prod_categories_delete;
/

--PROD_MENUS

CREATE OR REPLACE TRIGGER trg_prod_menus_insert
INSTEAD OF INSERT
ON vw_prod_menus
DECLARE
	pm_id number;
BEGIN
	
	INSERT INTO prod_menus(original_menu_id, menu_name, menu_description, original_mt_id, mt_name)
	VALUES (:new.original_menu_id, :new.menu_name, :new.menu_description, :new.original_mt_id, :new.mt_name)
	RETURNING prod_menus_id INTO pm_id;

	INSERT INTO prod_menus_history(prod_menus_id, menu_name, menu_description, mt_name)
	VALUES (pm_id, :new.menu_name, :new.menu_description, :new.mt_name);
END trg_prod_menus_insert;
/

CREATE OR REPLACE TRIGGER trg_prod_menus_update
INSTEAD OF UPDATE
ON vw_prod_menus
BEGIN

	IF :old.original_menu_id != :new.original_menu_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Keys in prod_menus do not match up');
	END IF;

	UPDATE prod_menus_history
	SET end_date = :new.from_date
	WHERE prod_menus_id = :old.prod_menus_id
	AND :new.from_date BETWEEN :old.from_date AND :old.end_date;

	UPDATE prod_menus
	SET menu_name = :new.menu_name, menu_description = :new.menu_description, original_mt_id = :new.original_mt_id, mt_name = :new.mt_name
	WHERE prod_menus_id = :old.prod_menus_id;

	INSERT INTO prod_menus_history(from_date, prod_menus_id, menu_name, menu_description, mt_name)
	VALUES (:new.from_date, :new.prod_menus_id, :new.menu_name, :new.menu_description, :new.mt_name);

END trg_prod_menus_update;
/

CREATE OR REPLACE TRIGGER trg_prod_menus_delete
INSTEAD OF DELETE
ON vw_prod_menus
BEGIN
	UPDATE prod_menus_history
	SET end_date = SYSDATE
	WHERE prod_menus_id = :old.prod_menus_id
	AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;
END trg_prod_menus_delete;
/

--PROD_CUSTOM_ITEMS

CREATE OR REPLACE TRIGGER trg_prod_custom_items_insert
INSTEAD OF INSERT
ON vw_prod_custom_items
DECLARE
	pc_id number;
	pm_id number;
BEGIN

	SELECT pc.prod_cat_id, pm.prod_menus_id INTO pc_id, pm_id FROM prod_categories pc, prod_menus pm WHERE pc.original_cat_id = :new.original_cat_id AND pm.original_menu_id = :new.original_menu_id;

	INSERT INTO prod_custom_items(original_item_id, name, subtitle, description, allergenes, prod_cat_id, prod_menus_id)
	VALUES (:new.original_item_id, :new.name, :new.subtitle, :new.description, :new.allergenes, pc_id, pm_id);
END trg_prod_custom_items_insert;
/

create or replace TRIGGER trg_prod_custom_items_update
INSTEAD OF UPDATE
ON vw_prod_custom_items
DECLARE
    pc_id number;
    pm_id number;

    v_old_pci_id number;
    v_new_pci_id number;
BEGIN

	IF :old.original_item_id != :new.original_item_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Keys in prod_custom_items do not match up');
	END IF;


	IF :NEW.from_date < :old.from_date THEN
        --Update olf data set
        UPDATE prod_custom_items
        SET end_date = :new.from_date
        WHERE original_item_id = :new.original_item_id AND end_date between :new.from_date and :old.from_date
        RETURNING prod_custom_items_id INTO v_old_pci_id;

    	--INSERT A NEW ROW WITH THE NEW DATA
	    INSERT INTO prod_custom_items(from_date, end_date, original_item_id, name, subtitle, description, allergenes, prod_cat_id, prod_menus_id)
		VALUES (:new.from_date, :old.from_date, :new.original_item_id, :new.name, :new.subtitle, :new.description, :new.allergenes, pc_id, pm_id)
	    RETURNING prod_custom_items_id INTO v_new_pci_id;
 
        dbms_output.put_line(v_old_pci_id);
        dbms_output.put_line(v_new_pci_id);

	    --Update elements in detailed item_sales_fact
	    UPDATE detailed_item_sales_fact
	    SET prod_custom_items_id = v_new_pci_id
	    WHERE prod_custom_items_id = v_old_pci_id AND
            EXISTS (
                SELECT disf.disf_guid
                FROM detailed_item_sales_fact disf
                JOIN dim_date dd on  dd.dim_date_id = disf.dim_date_id
                JOIN dim_time dt on dt.dim_time_id = disf.dim_time_id
                WHERE  to_date(dd.year || '.' ||LPAD(dd.month,2,'0') ||'.'|| LPAD(dd.day,2,'0')||' '||dt.hour||':'||dt.minute,'YYYY.MM.DD HH24:MI') between to_date(:new.from_date) and to_date(:old.from_date)
            );
    ELSIF :new.from_date > :old.from_date THEN

        --UPDATE THE END DATE OF THE CURRENT ELEMENT
        SELECT pc.prod_cat_id, pm.prod_menus_id INTO pc_id, pm_id FROM prod_categories pc, prod_menus pm WHERE pc.original_cat_id = :new.original_cat_id AND pm.original_menu_id = :new.original_menu_id;

		UPDATE prod_custom_items
		SET end_date = :new.from_date
		WHERE prod_custom_items_id = :old.prod_custom_items_id;

		INSERT INTO prod_custom_items(from_date, original_item_id, name, subtitle, description, allergenes, prod_cat_id, prod_menus_id)
		VALUES (:new.from_date, :new.original_item_id, :new.name, :new.subtitle, :new.description, :new.allergenes, pc_id, pm_id);
    ELSE
        RAISE_APPLICATION_ERROR(-20011,'ERROR');
    END IF;



END trg_prod_custom_items_update;
/

CREATE OR REPLACE TRIGGER trg_prod_custom_items_delete
INSTEAD OF DELETE
ON vw_prod_custom_items
BEGIN
	UPDATE prod_custom_items
	SET end_date = SYSDATE
	WHERE prod_custom_items_id = :old.prod_custom_items_id
	AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;
END trg_prod_custom_items_delete;
/

--DIM_EXTRA_ITEMS
/*
CREATE OR REPLACE TRIGGER trg_dim_extra_items_insert
INSTEAD OF INSERT
ON vw_dim_extra_items
BEGIN
	INSERT INTO dim_extra_items(original_exitem_id, name, price, allergenes)
	VALUES (:new.original_exitem_id, :new.name, :new.price, :new.allergenes);
END trg_dim_extra_items_insert;
/*/

CREATE OR REPLACE TRIGGER trg_dim_extra_items_update
INSTEAD OF UPDATE
ON vw_dim_extra_items
BEGIN

	IF :old.original_exitem_id != :new.original_exitem_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Keys in dim_extra_items do not match up');
	END IF;

	UPDATE dim_extra_items
	SET end_date = :new.from_date
	WHERE dim_extra_items_id = :old.dim_extra_items_id
	AND :new.from_date BETWEEN :old.from_date AND :old.end_date;

	INSERT INTO dim_extra_items(from_date, original_exitem_id, name, price, allergenes)
	VALUES (:new.from_date, :new.original_exitem_id, :new.name, :new.price, :new.allergenes);
END trg_dim_extra_items_update;
/

CREATE OR REPLACE TRIGGER trg_dim_extra_items_delete
INSTEAD OF DELETE
ON vw_dim_extra_items
BEGIN
	UPDATE dim_extra_items
	SET end_date = SYSDATE
	WHERE dim_extra_items_id = :old.dim_extra_items_id
	AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;
END trg_dim_extra_items_delete;
/

--DIM_EMPLOYEES
CREATE OR REPLACE TRIGGER trg_dim_employees_insert
INSTEAD OF INSERT
ON vw_dim_employees
DECLARE
	de_id number;
BEGIN
	INSERT INTO dim_employees(original_emp_id, last_name, first_name, middle_names, gender, birthdate, original_job_id, job_title)
	VALUES(:new.original_emp_id, :new.last_name, :new.first_name, :new.middle_names, :new.gender, :new.birthdate, :new.original_job_id, :new.job_title)
	RETURNING dim_employees_id INTO de_id;

	INSERT INTO dim_employees_history(dim_employees_id, last_name, first_name, middle_names, job_title)
	VALUES(de_id, :new.last_name, :new.first_name, :new.middle_names, :new.job_title);

END trg_dim_employees_insert;
/

create or replace TRIGGER trg_dim_employees_update
INSTEAD OF UPDATE
ON vw_dim_employees
BEGIN

	IF :old.original_emp_id != :new.original_emp_id THEN
		RAISE_APPLICATION_ERROR(-20000, 'Primary Keys in dim_employees do not match up');
	END IF;

	UPDATE dim_employees_history
	SET end_date = :new.from_date
	WHERE dim_employees_id = :old.dim_employees_id
	AND :new.from_date BETWEEN :old.from_date AND :old.end_date;

	UPDATE dim_employees
	SET last_name = :new.last_name, first_name = :new.first_name, middle_names = :new.middle_names, gender = :new.gender, birthdate = :new.birthdate, original_job_id = :new.original_job_id, job_title = :new.job_title
	WHERE dim_employees_id = :old.dim_employees_id;

	INSERT INTO dim_employees_history(from_date, dim_employees_id, last_name, first_name, middle_names, job_title)
	VALUES(:new.from_date, :new.dim_employees_id, :new.last_name, :new.first_name, :new.middle_names, :new.job_title);

END trg_dim_employees_update;
/

CREATE OR REPLACE TRIGGER trg_dim_employees_delete
INSTEAD OF DELETE
ON vw_dim_employees
BEGIN
	UPDATE dim_employees_history
	SET end_date = SYSDATE
	WHERE dim_employees_id = :old.dim_employees_id
	AND SYSTIMESTAMP BETWEEN :old.from_date AND :old.end_date;
END trg_dim_employees_delete;
/


--DIM_CUSTOMERS
/*
CREATE OR REPLACE TRIGGER trg_dim_customers_insert
INSTEAD OF INSERT
ON vw_dim_customers
DECLARE
	dc_id number;
BEGIN
	INSERT INTO dim_customers(original_pers_id, last_name, first_name, middle_names, gender, birthdate,email)
	VALUES (:new.original_pers_id, :new.last_name, :new.first_name, :new.middle_names, :new.gender, to_date(:new.birthdate,'DD.MM.YYYY'), UPPER(:new.email))
	RETURNING dim_customers_id INTO dc_id;

	INSERT INTO dim_customers_history(dim_customers_id, last_name, first_name, middle_names)
	VALUES (dc_id, :new.last_name, :new.first_name, :new.middle_names);
END trg_dim_customers_insert;
/

create or replace TRIGGER trg_dim_customers_update
INSTEAD OF UPDATE
ON vw_dim_customers
BEGIN

	IF :old.original_pers_id != :new.original_pers_id THEN
		RAISE_APPLICATION_ERROR(-20000,'Primary Keys in dim_customers do not match');
	END IF;

	UPDATE dim_customers_history
	SET end_date = :new.from_date
	WHERE dim_customers_id = :old.dim_customers_id
	AND :new.from_date BETWEEN :old.from_date AND :old.end_date;

	UPDATE dim_customers
	SET last_name = :new.last_name, first_name = :new.first_name, middle_names = :new.middle_names, gender = :new.gender, birthdate =  to_date(:new.birthdate,'DD.MM.YYYY'), email = UPPER(:new.email)
    WHERE dim_customers_id = :new.dim_customers_id;

	INSERT INTO dim_customers_history(from_date, dim_customers_id, last_name, first_name, middle_names)
	VALUES (:new.from_date,:new.dim_customers_id, :new.last_name, :new.first_name, :new.middle_names);
END trg_dim_customers_update;

/*/

CREATE OR REPLACE TRIGGER trg_dim_customers_delete
INSTEAD OF DELETE
ON vw_dim_customers
BEGIN
	dbms_output.put_line('Cannot Delete Element from Customers Dimension');

END trg_dim_customers_delete;
/