create or replace procedure generateRandomSalesData(start_date in varchar2, end_date in varchar2)
as
    v_guid  raw(36);

    v_start_date    timestamp;
    v_current_date  timestamp;
    v_current_end_date  timestamp;
    v_end_date      timestamp;

    v_seednumber number := 0;
    v_dim_sessions_id number;
    v_prod_items_id number;
    v_custom_items_id number;
    v_quantity_sold number;
    v_original_mt_id number;
    v_dim_date_id number;
    v_dim_time_id number;
    v_dim_employees_id number;
    v_dim_customers_id number;
    v_prod_menus_id number;
    v_original_item_id number;

    v_hasSelection number;
    CURSOR cur_items_selg(cp_item_id number) IS SELECT selg_id FROM(SELECT selg_id from operational.items_selgs WHERE item_id = cp_item_id ORDER BY dbms_random.value) WHERE rownum = 1;
    rec_items_selg      cur_items_selg%ROWTYPE;

    v_original_sel_id number;
    v_selg_id number;
    v_dim_selections_id number;
    v_isf_guid raw(36);

    v_sum number;
    v_price number;

    CURSOR cur_selgSel(cp_item_id number) is SELECT selg_id from operational.items_selgs where item_id = cp_item_id;
    rec_selgSel cur_selgSel%ROWTYPE;
begin

    UPDATE prod_items_history
    SET from_date = to_date(start_date,'DD.MM.YYYY');

    UPDATE prod_categories_history
    SET from_date = to_date(start_date,'DD.MM.YYYY');

    UPDATE prod_menus_history
    SET from_date = to_date(start_date,'DD.MM.YYYY');

    UPDATE dim_employees_history
    SET from_date = to_date(start_date,'DD.MM.YYYY');

    UPDATE dim_selections
    SET from_date = to_date(start_date,'DD.MM.YYYY');

    UPDATE prod_custom_items
    SET from_date = to_date(start_date,'DD.MM.YYYY');

    UPDATE dim_extra_items
    SET from_date = to_date(start_date,'DD.MM.YYYY');



    v_start_date := to_timestamp(start_date,'DD.MM.YYYY');
    v_end_date := to_timestamp(end_date,'DD.MM.YYYY');

    while v_start_date <= v_end_date
    loop
        for n IN 4..dbms_random.value(5,30)
        loop
            --Renew Seed
            DBMS_RANDOM.SEED(v_seednumber);
            v_seednumber := v_seednumber + 83;

            --Get GUID
            SELECT SYS_GUID()INTO v_guid FROM dual;

            --No Entries for Saturday and Sundays
            if to_number(to_char(v_start_date, 'D')) = 6 THEN
                v_start_date := v_start_date+2;
                continue;--todo fall out of main lloop
            end if;

            if to_number(to_char(v_start_date, 'D')) = 7 THEN
                v_start_date := v_start_date+1;
                continue;
            end if;

            --Generate Session Start and End
            v_current_date := v_start_date + 1/24 * dbms_random.value(11.75,13.75);
            v_current_end_date := v_current_date  + 1/24 * dbms_random.value(1.0,2.5);

            --Insert Session and retriebe dim_sessions_id
            INSERT INTO DIM_SESSIONS (SESSION_GUID, SESSION_START, SESSION_CLOSED, SESSION_STATUS) VALUES ( v_guid, to_timestamp(v_current_date), to_timestamp(v_current_end_date),'P') RETURNING dim_sessions_id INTO v_dim_sessions_id;

            --Retrieve dim_date_id
            SELECT dim_date_id INTO v_dim_date_id FROM dim_date WHERE year = extract(year from v_current_date) AND MONTH = extract(month from v_current_date) AND DAY = extract(day from v_current_date);

            --Retrieve dim_time_id (Always session start time) %TODO MORE FLEXIBLE TIMES
            SELECT dim_time_id INTO v_dim_time_id FROM dim_time WHERE hour = extract(hour from v_current_date) and minute = extract(minute from v_current_date);

            --Retrieve random employee
            SELECT dim_employees_id INTO v_dim_employees_id FROM( SELECT dim_employees_id FROM dim_employees ORDER BY dbms_random.value ) WHERE rownum = 1;

            --Retrieve random customer
            if dbms_random.value(0,1) > 0.4 then
                SELECT dim_customers_id INTO v_dim_customers_id FROM( SELECT dim_customers_id FROM dim_customers ORDER BY dbms_random.value ) WHERE rownum = 1;
            else
                v_dim_customers_id := -404;
            end if;
            --Create per GUID 2 - 10 Rows in the Item_sales_fact
            for isf in 2..dbms_random.value(3,10)
            loop
                --Retrieve random item
                SELECT prod_items_id, price, original_item_id INTO v_prod_items_id,v_price, v_original_item_id FROM( SELECT prod_items_id, price, original_item_id FROM prod_items WHERE prod_items_id > 0 ORDER BY dbms_random.value ) WHERE rownum = 1;

                --Retrieve MT_ID and prod_menus_id for random item
                SELECT pm.original_mt_id, pm.prod_menus_id INTO v_original_mt_id, v_prod_menus_id FROM prod_menus pm JOIN prod_items pi ON pi.prod_menus_id = pm.prod_menus_id and pi.prod_items_id = v_prod_items_id;

                --Check if Item has a Selection Available
                BEGIN
                    SELECT count(its.item_id) INTO v_hasSelection FROM (SELECT original_item_id FROM prod_items WHERE prod_items_id = v_prod_items_id) m, operational.items_selgs its WHERE its.item_id = m.original_item_id;
                    EXCEPTION 
                        WHEN NO_DATA_FOUND THEN v_hasSelection := 0;
                END;

                --Retrieve a random quantity sold
                v_quantity_sold := round(dbms_random.value(1,5),0);

                --INSERT into item_sales_fact
                INSERT INTO ITEM_SALES_FACT(QUANTITY_SOLD,PRICE,DIM_DATE_ID, DIM_TIME_ID, PROD_ITEMS_ID, DIM_EMPLOYEES_ID, DIM_SESSIONS_ID, DIM_CUSTOMERS_ID) VALUES(v_quantity_sold,v_price,v_dim_date_id,v_dim_time_id,v_prod_items_id,v_dim_employees_id,v_dim_sessions_id,v_dim_customers_id ) RETURNING isf_guid INTO v_isf_guid ;

                --If Menu is customizable
                if v_original_mt_id = 2 then
                    --for as many as in quantity_sold
                    for i IN 1..v_quantity_sold loop
                        --select random prod_custom_items_id %TODO CHECK FOR WEIGHT IN OPERATIONAL DATABASE
                        SELECT prod_custom_items_id INTO v_custom_items_id FROM( SELECT prod_custom_items_id FROM prod_custom_items WHERE prod_menus_id = v_prod_menus_id ORDER BY dbms_random.value ) WHERE rownum = 1;
                        --INSERT INTO DETAILED
                        INSERT INTO DETAILED_ITEM_SALES_FACT(ISF_GUID, DIM_DATE_ID, DIM_TIME_ID, PROD_ITEMS_ID, DIM_EMPLOYEES_ID, DIM_SESSIONS_ID, DIM_CUSTOMERS_ID, PROD_CUSTOM_ITEMS_ID, DIM_SELECTIONS_ID, DIM_EXTRA_ITEMS_ID) VALUES(v_isf_guid, v_dim_date_id  , v_dim_time_id , v_prod_items_id , v_dim_employees_id  ,v_dim_sessions_id,  v_dim_customers_id , v_custom_items_id ,-1,-1);
                    end loop;
                end if;

                if v_hasSelection != 0 then
                    open cur_selgSel(v_original_item_id);
                    loop
                        fetch cur_selgSel INTO rec_selgSel;
                        exit when cur_selgSel%NOTFOUND;

                        v_selg_id := rec_selgSel.selg_id;
                        SELECT original_sel_id INTO v_original_sel_id FROM(SELECT original_sel_id FROM DIM_SELECTIONS WHERE original_selg_id = v_selg_id ORDER BY dbms_random.value) where rownum = 1;

                        SELECT dim_selections_id INTO v_dim_selections_id FROM dim_selections WHERE original_selg_id = v_selg_id AND original_sel_id = v_original_sel_id;
                        INSERT INTO DETAILED_ITEM_SALES_FACT(ISF_GUID, DIM_DATE_ID, DIM_TIME_ID, PROD_ITEMS_ID, DIM_EMPLOYEES_ID, DIM_SESSIONS_ID, DIM_CUSTOMERS_ID, PROD_CUSTOM_ITEMS_ID, DIM_SELECTIONS_ID, DIM_EXTRA_ITEMS_ID) VALUES(v_isf_guid, v_dim_date_id , v_dim_time_id , v_prod_items_id,v_dim_employees_id ,v_dim_sessions_id,  v_dim_customers_id ,-1, v_dim_selections_id ,-1);
               
                    end loop;
                    close cur_selgSel;
                end if;
            end loop;

            SELECT SUM(isf.quantity_sold * isf.price) INTO v_sum FROM item_sales_fact isf join prod_items pi on pi.prod_items_id = isf.prod_items_id where isf.dim_sessions_id = v_dim_sessions_id;

            INSERT INTO payment_fact(price_paid, bonus_points, dim_date_id, dim_time_id, dim_sessions_id,dim_employees_id, dim_payment_methods_id, dim_customers_id) VALUES (v_sum, v_sum*0.05, v_dim_date_id, v_dim_time_id, v_dim_sessions_id, v_dim_employees_id, round(dbms_random.value(1,3),0), v_dim_customers_id);

        end loop;
        v_start_date := v_start_date+1;
    end loop;

    commit;
end;


