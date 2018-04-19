CREATE OR REPLACE PACKAGE pkg_inventory_management
AS
	--MENU_TYPE
	procedure insert_menu_type(p_name in varchar2, p_json_tag in varchar2, p_weighted in number); --ok
	procedure update_menu_type(p_mt_id in number, p_name in varchar2, p_json_tag in varchar2, p_weighted in number); --ok
	procedure delete_menu_type(p_mt_id in number);--ok

	--MENUS
	procedure insert_menu(p_mt_id in number, p_name in varchar2, p_description in varchar2); --ok
	procedure update_menu(p_menu_id in number, p_mt_id in number, p_name in varchar2, p_description in varchar2); --ok
	procedure delete_menu(p_menu_id in number);--ok

	--NESTED_CATEGORIES
	procedure insert_nested_category(p_menu_id in number, p_name in varchar2, p_description in varchar2, p_lft in number, p_rgt in number, p_weight in number); -- ok
	procedure update_nested_category(p_cat_id in number, p_menu_id in number, p_name in varchar2, p_description in varchar2, p_lft in number, p_rgt in number, p_weight in number); --ok
	procedure delete_nested_category(p_cat_id in number);--ok

	--ITEMS
	procedure insert_item(p_name in varchar2, p_subtitle in varchar2, p_description in varchar2, p_allergenes in number); --ok
	procedure update_item(p_item_id in number, p_name in varchar2, p_subtitle in varchar2, p_description in varchar2, p_allergenes in number); --ok
	procedure delete_item(p_item_id in number); --ok

	--CONNECT ITEMS TO CATS
	procedure add_item_to_category(p_item_id in number, p_cat_id in number); --ok
	procedure remove_item_from_category(p_item_id in number); --ok

	--ITEM_PRICES
	procedure insert_item_price(p_item_id in number, p_price in number, p_price_name in varchar2); --ok
	procedure update_item_price(p_item_id in number, p_oldPrice in number, p_price in number, p_price_name in varchar2); --ok
	procedure delete_item_price(p_item_id in number, p_price in number); --ok

	--W_MENUS_PRICES
	procedure insert_weighted_menu(p_menu_id in number, p_weight in number, p_item_id in number); --ok
	procedure delete_weigthed_menu(p_menu_id in number, p_item_id in number); --ok

	--EXTRA_ITEMS
	procedure insert_extra_item(p_name in varchar2, p_allergenes in number, p_price in number); --ok
	procedure update_extra_item(p_exitem_id in number, p_name in varchar2, p_allergenes in number, p_price in number); --ok
	procedure delete_extra_item(p_exitem_id in number);

	--ITEMS_EITEMS
	procedure add_extraitem_to_item(p_exitem_id in number, p_item_id in number);
	procedure remove_extraitem_from_item(p_exitem_id in number, p_item_id in number);

	--SELECTION_GROUPS
	procedure insert_selection_group(p_name in varchar2); --ok
	procedure update_selection_group(p_selg_id in number, p_name in varchar2); --ok
	procedure delete_selection_group(p_selg_id in number); --ok

	--SELECTIONS
	procedure insert_selection(p_name in varchar2, p_description in varchar2); --ok
	procedure update_selection(p_sel_id in number, p_name in varchar2, p_description in varchar2);--ok
	procedure delete_selection(p_sel_id in number);--ok

	--SELGS_SELS
	procedure add_selection_to_selectiongroup(p_sel_id in number, p_selg_id in number); --ok
	procedure remove_selection_from_selectiongroup(p_sel_id in number, p_selg_id in number); --ok

	--ITEMS_SELGS
	procedure add_selectiongroup_to_item(p_selg_id in number, p_item_id in number); --ok
	procedure remove_selectiongroup_from_item(p_selg_id in number, p_item_id in number);--ok

END pkg_inventory_management;
/
create or replace PACKAGE BODY pkg_inventory_management
AS

	--MENU_TYPE
	procedure insert_menu_type(p_name in varchar2, p_json_tag in varchar2, p_weighted in number)
	is
	begin
		INSERT INTO menu_types(name, json_tag, weighted) VALUES (p_name, p_json_tag, p_weighted);
		commit;
	end insert_menu_type;


	procedure update_menu_type(p_mt_id in number, p_name in varchar2, p_json_tag in varchar2, p_weighted in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		UPDATE menu_types
		SET name = p_name, json_tag = p_json_tag, weighted = p_weighted
		WHERE mt_id = p_mt_id
		RETURNING rowid, mt_id INTO v_rowid, v_changedRow;

		IF v_changedRow = p_mt_id THEN
			v_json := create_etl_json( 'UPDATE', 'PROD_MENUS', '{"original_mt_id":"' || p_mt_id ||'"}', '{"mt_name":"' || p_name || '"}');
			--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
			log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003, 'Menu Type ' || p_mt_id || ' does not exist.');
		END IF;

		commit;
	end update_menu_type;


	procedure delete_menu_type(p_mt_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_references 			number;
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp; 

		SELECT count(menu_id) INTO v_references FROM menus WHERE mt_id = p_mt_id;

		IF v_references != 0 THEN
			RAISE_APPLICATION_ERROR(-20001, 'Menu Type ' || p_mt_id || ' is still referenced. Remove Menus first.');
		END IF;

		DELETE FROM menu_types WHERE mt_id = p_mt_id RETURNING rowid, mt_id INTO v_rowid, v_changedRow;

		IF v_changedRow = p_mt_id THEN
			v_json := create_etl_json( 'DELETE', 'PROD_MENUS', '{"original_mt_id":"' || p_mt_id ||'"}', null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
			log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003, 'Menu Type ' || p_mt_id || ' does not exist.');
		END IF;

		commit;

	end delete_menu_type;

	--MENUS
	procedure insert_menu(p_mt_id in number, p_name in varchar2, p_description in varchar2)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_mid					number;
		v_mt_name				varchar2(50);
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		INSERT INTO menus(mt_id, name, description) VALUES (p_mt_id, p_name, p_description) RETURNING rowid, menu_id INTO v_rowid, v_mid;

		SELECT name INTO v_mt_name FROM menu_types WHERE mt_id = p_mt_id;

		v_json := create_etl_json( 'INSERT', 'PROD_MENUS', '{"original_menu_id":"' || v_mid || '","original_mt_id":"' || p_mt_id || '"}', '{"menu_name":"' || p_name || '", "menu_description":"' || p_description || '", "mt_name":"' || v_mt_name || '"}');
		--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
		log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);

		commit;
	end insert_menu;

	procedure update_menu(p_menu_id in number, p_mt_id in number, p_name in varchar2, p_description in varchar2)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		UPDATE menus
		SET mt_id = p_mt_id, name = p_name, description = p_description
		WHERE menu_id = p_menu_id
		RETURNING rowid, menu_id INTO v_rowid, v_changedRow;

		IF v_changedRow = p_menu_id THEN
			v_json := create_etl_json( 'UPDATE', 'PROD_MENUS', '{"original_menu_id":"' || p_menu_id || '"}', '{"original_mt_id":"' || p_mt_id || '","menu_name":"' || p_name || '", "menu_description":"' || p_description || '"}');
			--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
			log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003, 'Menu ' || p_menu_id || ' does not exist.');
		END IF;

		commit;
	end;

	procedure delete_menu(p_menu_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_references			number;
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		SELECT SUM(count) INTO v_references FROM (SELECT count(cat_id) as count FROM nested_categories WHERE menu_id = p_menu_id UNION SELECT count(item_id) as count FROM w_menus_prices WHERE menu_id = p_menu_id);

		IF v_references != 0 THEN
			RAISE_APPLICATION_ERROR(-20001,'Menu ' || p_menu_id || ' is still referenced. Remove Categories first.');
		END IF;

		DELETE FROM menus
		WHERE menu_id = p_menu_id
		RETURNING rowid, menu_id INTO v_rowid, v_changedRow;

		IF v_changedRow = p_menu_id THEN
			v_json := create_etl_json( 'DELETE', 'PROD_MENUS', '{"original_menu_id":"' || p_menu_id || '"}', null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
			log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003,'Menu ' || p_menu_id || ' does not exist.');
		END IF;
		commit;
	end;

	--NESTED_CATEGORIES
	procedure insert_nested_category(p_menu_id in number, p_name in varchar2, p_description in varchar2, p_lft in number, p_rgt in number, p_weight in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_cat_id				number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		INSERT INTO nested_categories(menu_id, name, description, lft, rgt, weight) VALUES (p_menu_id, p_name, p_description, p_lft, p_rgt, p_weight) RETURNING rowid, cat_id INTO v_rowid, v_cat_id;
		commit;

		v_json := create_etl_json( 'INSERT', 'PROD_CATEGORIES', '{"original_cat_id":"' || v_cat_id || '"}', '{"name":"' || p_name || '", "description":"' || p_description || '", "lft":"' || p_lft || '", "rgt":"' || p_rgt || '","original_menu_id":"' || p_menu_id || '"}');
		--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
		log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
	end;

	procedure update_nested_category(p_cat_id in number, p_menu_id in number, p_name in varchar2, p_description in varchar2, p_lft in number, p_rgt in number, p_weight in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		UPDATE nested_categories
		SET menu_id = p_menu_id, name = p_name, description = p_description, lft = p_lft, rgt = p_rgt, weight = p_weight
		WHERE cat_id = p_cat_id
		RETURNING rowid, cat_id INTO v_rowid, v_changedRow;

		IF v_changedRow = p_cat_id THEN
			
			v_json := create_etl_json( 'UPDATE', 'PROD_CATEGORIES','{"original_cat_id":"' || p_cat_id || '"}','{"original_menu_id":"' || p_menu_id || '", "name":"' || p_name || '", "description":"' || p_description || '", "lft":"' || p_lft || '", "rgt":"' || p_rgt || '"}');
			--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
			log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003, 'Nested Category ' || p_cat_id || ' does not exist.');
		END IF;

		commit;
	end;

	procedure delete_nested_category(p_cat_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_references			number;
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;

		SELECT count(item_id) INTO v_references FROM items WHERE cat_id = p_cat_id;

		IF v_references != 0 THEN
			RAISE_APPLICATION_ERROR(-20001, 'Nested Category ' ||p_cat_id|| ' is still referenced. Remove Items first.');
		END IF;

		DELETE FROM nested_categories
		WHERE cat_id = p_cat_id
		RETURNING rowid, cat_id INTO v_rowid, v_changedRow;

		IF v_changedRow = p_cat_id THEN
			
			v_json := create_etl_json( 'DELETE', 'PROD_CATEGORIES', '{"original_cat_id":"' || p_cat_id || '"}', null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
			log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003, 'Nested Category ' || p_cat_id || ' does not exist.');
		END IF;

		commit;
	end;

	--ITEMS
	procedure insert_item(p_name in varchar2, p_subtitle in varchar2, p_description in varchar2, p_allergenes in number)
	is
	begin
		INSERT INTO items(name, subtitle, description, allergenes) VALUES (p_name, p_subtitle, p_description, p_allergenes);
		commit;
	end;

	procedure update_item(p_item_id in number, p_name in varchar2, p_subtitle in varchar2, p_description in varchar2, p_allergenes in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		v_weight				number;
		v_weight2				number;
		v_menu_id				number;
		v_cat_id				number;
		v_price 				number;
		v_price_name			varchar2(50);
		v_changedRow			number;
		v_rowid 				ROWID;
	begin
		v_current_timestamp := systimestamp;
		BEGIN
			SELECT weighted, menu_id, cat_id INTO v_weight, v_menu_id, v_cat_id FROM (SELECT mt.weighted, m.menu_id, nc.cat_id FROM menu_types mt JOIN menus m on m.mt_id = mt.mt_id JOIN nested_categories nc on nc.menu_id = m.menu_id JOIN items it on it.cat_id = nc.cat_id WHERE it.item_id = p_item_id);
			EXCEPTION
				WHEN NO_DATA_FOUND THEN v_weight := -1;
		END;

		UPDATE items
		SET name = p_name, subtitle = p_subtitle, description = p_description, allergenes = p_allergenes
		WHERE item_id = p_item_id
		RETURNING rowid, item_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_item_id THEN
			RAISE_APPLICATION_ERROR(-20003,'Item ' || p_item_id || ' does not exist.');
		END IF;

		

		IF v_weight = 0 OR v_weight = -1 THEN
			v_json := create_etl_json( 'UPDATE', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || p_name || '", "subtitle":"' || p_subtitle || '", "description":"' || p_description || '", "allergenes":"' || p_allergenes || '", "original_cat_id":"' || v_cat_id || '", "original_menu_id":"' || v_menu_id || '"}');
			--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
		ELSIF v_weight = 1 THEN--CUSTOMITEMS
			v_json := create_etl_json( 'UPDATE', 'PROD_CUSTOM_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || p_name || '", "subtitle":"' || p_subtitle || '", "description":"' || p_description || '", "allergenes":"' || p_allergenes || '", "original_cat_id":"' || v_cat_id || '", "original_menu_id":"' || v_menu_id || '"}');
			--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
		END IF;

		log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);

		commit;
	end;


	procedure delete_item(p_item_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow			number;

		v_weight number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		BEGIN
			SELECT weighted INTO v_weight FROM (SELECT mt.weighted FROM menu_types mt JOIN menus m on m.mt_id = mt.mt_id JOIN nested_categories nc on nc.menu_id = m.menu_id JOIN items it on it.cat_id = nc.cat_id WHERE it.item_id = p_item_id);
			EXCEPTION
				WHEN NO_DATA_FOUND THEN v_weight := -1;
		END;

		DELETE FROM items
		WHERE item_id = p_item_id
		RETURNING rowid, item_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_item_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Item ' || p_item_id || ' does not exist.');
		END IF;

		

		IF v_weight = 0 OR v_weight = -1 THEN
			v_json := create_etl_json( 'DELETE', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}',null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
		ELSIF v_weight = 1 THEN--CUSTOMITEMS
			v_json := create_etl_json( 'DELETE', 'PROD_CUSTOM_ITEMS','{"original_item_id":"' || p_item_id || '"}',null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
		end if;
		log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

	end;

	--CONNECT ITEMS TO CATS

	procedure add_item_to_category(p_item_id in number, p_cat_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		v_weight				number;
		v_menu_id				number;
		v_cat_id				number;
		CURSOR cur_prices(cp_item_id number) IS SELECT rowid, price, price_name FROM item_prices WHERE item_id = cp_item_id;
		ref_prices		cur_prices%ROWTYPE;

        v_name                  varchar2(50);
        v_subtitle              varchar2(100);
        v_description           varchar2(1000);
        v_allergenes            number;

        v_changedRow			number;
        v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		UPDATE items
		SET cat_id = p_cat_id
		WHERE item_id = p_item_id
		RETURNING rowid, item_id INTO v_rowid, v_changedRow;


		IF v_changedRow != p_item_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Item ' || p_item_id || ' does not exist.');
		END IF;

		

		BEGIN
			SELECT weighted, menu_id, cat_id, name, subtitle, description, allergenes INTO v_weight, v_menu_id, v_cat_id, v_name, v_subtitle, v_description, v_allergenes FROM (SELECT mt.weighted, m.menu_id, nc.cat_id, it.name, it.subtitle, it.description, it.allergenes FROM menu_types mt JOIN menus m on m.mt_id = mt.mt_id JOIN nested_categories nc on nc.menu_id = m.menu_id JOIN items it on it.cat_id = nc.cat_id WHERE it.item_id = p_item_id);
			EXCEPTION
				WHEN NO_DATA_FOUND THEN v_weight := -1;
		END;

		IF v_weight = 0 THEN
			OPEN cur_prices(p_item_id);
			LOOP
				FETCH cur_prices INTO ref_prices;
				EXIT WHEN cur_prices%NOTFOUND;
				v_current_timestamp := systimestamp;
				v_json := create_etl_json( 'INSERT', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || v_name || '", "subtitle":"' || v_subtitle || '", "description":"' || v_description || '", "price":"' || ref_prices.price || '", "price_name":"' || ref_prices.price_name || '", "allergenes":"' || v_allergenes || '", "original_cat_id":"' || v_cat_id || '", "original_menu_id":"' || v_menu_id || '"}');
				--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
				v_rowid := ref_prices.rowid;
				log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
			END LOOP;
		ELSIF v_weight = 1 THEN--CUSTOMITEMS
			v_json := create_etl_json( 'INSERT', 'PROD_CUSTOM_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || v_name || '", "subtitle":"' || v_subtitle || '", "description":"' || v_description || '", "allergenes":"' || v_allergenes || '", "original_cat_id":"' || v_cat_id || '", "original_menu_id":"' || v_menu_id || '"}');
			--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
			log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		END IF;

		commit;
	end;

	procedure remove_item_from_category(p_item_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_weight				number;
		v_changedRow			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		BEGIN
			SELECT weighted INTO v_weight FROM (SELECT mt.weighted FROM menu_types mt JOIN menus m on m.mt_id = mt.mt_id JOIN nested_categories nc on nc.menu_id = m.menu_id JOIN items it on it.cat_id = nc.cat_id WHERE it.item_id = p_item_id);
			EXCEPTION
				WHEN NO_DATA_FOUND THEN v_weight := -1;
		END;

		UPDATE items
		SET cat_id = null
		WHERE item_id = p_item_id RETURNING rowid, item_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_item_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Item ' || p_item_id || 'does not exist.');
		END IF;

		

		IF v_weight = 0 THEN
			v_json := create_etl_json( 'DELETE', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}',null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
			log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSIF v_weight = 1 THEN--CUSTOMITEMS
			v_json := create_etl_json( 'DELETE', 'PROD_CUSTOM_ITEMS','{"original_item_id":"' || p_item_id || '"}',null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
			log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		END IF;

		commit;
	end;

	--ITEM_PRICES
	procedure insert_item_price(p_item_id in number, p_price in number, p_price_name in varchar2)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		v_weight				number;
		v_weight2				number;
		v_menu_id				number;
		v_cat_id				number;

        v_name                  varchar2(50);
        v_subtitle              varchar2(100);
        v_description           varchar2(1000);
        v_allergenes            number;

        v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		INSERT INTO item_prices VALUES (p_item_id, p_price, p_price_name) RETURNING rowid INTO v_rowid;
		


		BEGIN
			SELECT weighted, menu_id, cat_id, name, subtitle, description, allergenes INTO v_weight, v_menu_id, v_cat_id, v_name, v_subtitle, v_description, v_allergenes FROM (SELECT mt.weighted, m.menu_id, nc.cat_id, it.name, it.subtitle, it.description, it.allergenes FROM menu_types mt JOIN menus m on m.mt_id = mt.mt_id JOIN nested_categories nc on nc.menu_id = m.menu_id JOIN items it on it.cat_id = nc.cat_id WHERE it.item_id = p_item_id);
			EXCEPTION
				WHEN NO_DATA_FOUND THEN v_weight := -1;
		END;

		IF v_weight = 0 THEN
			v_json := create_etl_json( 'INSERT', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || v_name || '", "subtitle":"' || v_subtitle || '", "description":"' || v_description || '", "price":"' || p_price || '", "price_name":"' || p_price_name || '", "allergenes":"' || v_allergenes || '", "original_cat_id":"' || v_cat_id || '", "original_menu_id":"' || v_menu_id || '"}');
			--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
			log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSIF v_weight = 1 THEN
			RAISE_APPLICATION_ERROR(-20004, 'Cannot add a price to Item ' || p_item_id || '.');
		ELSIF v_weight = -1 THEN --weighted menus
			BEGIN
				SELECT wmp.weight, it.name, it.subtitle, it.description, it.allergenes, wmp.menu_id  INTO v_weight2, v_name, v_subtitle, v_description, v_allergenes, v_menu_id FROM w_menus_prices wmp JOIN items it ON it.item_id = wmp.item_id WHERE wmp.item_id = p_item_id;

				EXCEPTION
					WHEN NO_DATA_FOUND THEN v_weight2 := -1;
			END;
			IF v_weight2 != -1 THEN
				v_json := create_etl_json( 'INSERT', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || v_name || '", "subtitle":"' || v_subtitle || '", "description":"' || v_description || '", "price":"' || p_price || '", "price_name":"' || p_price_name || '", "allergenes":"' || v_allergenes || '", "original_cat_id":null, "original_menu_id":"' || v_menu_id || '"}');
				--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
				log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
			END IF;
		END IF;

		commit;

	end;

	procedure update_item_price(p_item_id in number, p_oldPrice in number, p_price in number, p_price_name in varchar2)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		v_oldPrice				number;
		v_changedRow			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		UPDATE item_prices 
		SET price = p_price, price_name = p_price_name
		WHERE item_id = p_item_id and price = p_oldPrice
		RETURNING rowid, item_id INTO v_rowid, v_changedRow;


		IF v_changedRow != p_item_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Item ' || p_item_id || ' does not exist.');
		END IF;

		

		v_json := create_etl_json( 'UPDATE', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '","price":"' || p_oldPrice || '"}','{"price":"' || p_price || '", "price_name":"' || p_price_name || '"}');
		--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
		log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);

		commit;
	end;

	procedure delete_item_price(p_item_id in number, p_price in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		v_changedRowID			number;
		v_changedRowPrice		number;

		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		DELETE FROM item_prices WHERE item_id = p_item_id AND price = p_price RETURNING item_id, price INTO v_changedRowID, v_changedRowPrice;

		IF v_changedRowID = p_item_id AND v_changedRowPrice = p_price THEN
			
			v_json := create_etl_json( 'DELETE', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '","price":"' || p_price || '"}',null);
			--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
			log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		ELSE
			RAISE_APPLICATION_ERROR(-20003, 'Item ' || p_item_id || ' with price ' || p_price || ' does not exist.');
		END IF;

		commit;
	end;

	--W_MENUS_PRICES
	procedure insert_weighted_menu(p_menu_id in number, p_weight in number, p_item_id in number)
	is
        v_current_timestamp     timestamp;
        v_json                  clob;
        v_prices                number;

        v_name                  varchar2(50);
        v_subtitle              varchar2(100);
        v_description           varchar2(1000);
        v_allergenes            number;
        v_price                 number;
        v_price_name            varchar2(50);
        v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		SELECT count(price) INTO v_prices FROM item_prices WHERE item_id = p_item_id;

		IF v_prices = 0 THEN
			RAISE_APPLICATION_ERROR(-20002, 'The selected Item does not have a price. Add a Price First.');
		ELSIF v_prices > 1 THEN
			RAISE_APPLICATION_ERROR(-20005, 'Too Many Prices available for Item. Need just 1.');
		ELSIF v_prices = 1 THEN
			INSERT INTO w_menus_prices VALUES (p_menu_id, p_weight, p_item_id);
			

			SELECT rowid, name, subtitle, description, allergenes, price, price_name INTO v_rowid, v_name, v_subtitle, v_description, v_allergenes, v_price, v_price_name FROM (SELECT it.name, it.subtitle, it.description, it.allergenes, ip.price, ip.price_name FROM items it JOIN item_prices ip on ip.item_id = it.item_id WHERE it.item_id = p_item_id);

			v_json := create_etl_json( 'INSERT', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}','{"name":"' || v_name || '", "subtitle":"' || v_subtitle || '", "description":"' || v_description || '", "price":"' || v_price || '", "price_name":"' || v_price_name || '", "allergenes":"' || v_allergenes || '", "original_cat_id":"-1", "original_menu_id":"' || p_menu_id || '"}');
			--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
			log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
			commit;

		END IF;

	end;
	procedure delete_weigthed_menu(p_menu_id in number, p_item_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		DELETE FROM w_menus_prices
		WHERE menu_id = p_menu_id AND item_id = p_item_id
		RETURNING menu_id INTO v_changedRow;

		SELECT rowid INTO v_rowid FROM items WHERE item_id = p_item_id;

		IF v_changedRow != p_menu_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Not working');
		END IF;

		
		v_json := create_etl_json( 'DELETE', 'PROD_ITEMS','{"original_item_id":"' || p_item_id || '"}',null);
		--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
		log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

	end;

	--EXTRA_ITEMS
	procedure insert_extra_item(p_name in varchar2, p_allergenes in number, p_price in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_exitem_id				number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		INSERT INTO extra_items(name, allergenes, price) VALUES (p_name, p_allergenes, p_price) RETURNING rowid, exitem_id INTO v_rowid, v_exitem_id;

		v_json := create_etl_json( 'INSERT', 'DIM_EXTRA_ITEMS','{"original_exitem_id":"' || v_exitem_id || '"}','{"name":"' || p_name || '", "price":"' || p_price || '", "allergenes":"' || p_allergenes || '"}');
		--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end;

	procedure update_extra_item(p_exitem_id in number, p_name in varchar2, p_allergenes in number, p_price in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		UPDATE extra_items
		SET name = p_name, allergenes = p_allergenes, price = p_price
		WHERE exitem_id = p_exitem_id
		RETURNING rowid, exitem_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_exitem_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Extra Item does not exist.');
		END IF;

		

		v_json := create_etl_json( 'UPDATE', 'DIM_EXTRA_ITEMS','{"original_exitem_id":"' || p_exitem_id || '"}', '{"name":"' || p_name || '", "price":"' || p_price || '", "allergenes":"' || p_allergenes || '"}');
		--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

	end;

	procedure delete_extra_item(p_exitem_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		DELETE FROM extra_items
		WHERE exitem_id = p_exitem_id
		RETURNING rowid, exitem_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_exitem_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Extra Item does not exist.');
		END IF;

		

		v_json := create_etl_json( 'DELETE', 'DIM_EXTRA_ITEMS','{"original_exitem_id":"' || p_exitem_id || '"}', null);
		--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

	end;

	--ITEMS_EITEMS
	procedure add_extraitem_to_item(p_exitem_id in number, p_item_id in number)
	is
	begin
		INSERT INTO items_eitems VALUES(p_item_id, p_exitem_id);
		commit;
	end;

	procedure remove_extraitem_from_item(p_exitem_id in number, p_item_id in number)
	is
	begin
		DELETE FROM items_eitems WHERE item_id = p_item_id AND exitem_id = p_exitem_id;
		commit;
	end;

	--SELECTION_GROUPS
	procedure insert_selection_group(p_name in varchar2)
	is
	begin
		INSERT INTO selection_groups(name) VALUES(p_name);
		commit;
	end;

	procedure update_selection_group(p_selg_id in number, p_name in varchar2)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin

		v_current_timestamp := systimestamp;

		UPDATE selection_groups
		SET name = p_name
		WHERE selg_id = p_selg_id
		RETURNING rowid, selg_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_selg_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Selection Group doesnt exist.');
		END IF;

		

		v_json := create_etl_json( 'UPDATE', 'DIM_SELECTIONS','{"original_selg_id":"' || p_selg_id || '"}', '{"selg_name":"' || p_name || '"}');
		--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end;

	procedure delete_selection_group(p_selg_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin

		v_current_timestamp := systimestamp;

		DELETE FROM selection_groups
		WHERE selg_id = p_selg_id
		RETURNING rowid, selg_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_selg_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Selection Group doesnt exist.');
		END IF;

		

		v_json := create_etl_json( 'DELETE', 'DIM_SELECTIONS','{"original_selg_id":"' || p_selg_id || '"}', null);
		--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

	end;

	--SELECTIONS
	procedure insert_selection(p_name in varchar2, p_description in varchar2)
	is
	begin
		INSERT INTO selections(name, description) VALUES (p_name, p_description);
		commit;
	end;

	procedure update_selection(p_sel_id in number, p_name in varchar2, p_description in varchar2)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin

		v_current_timestamp := systimestamp;

		UPDATE selections
		SET name = p_name, description = p_description
		WHERE sel_id = p_sel_id
		RETURNING rowid, sel_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_sel_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Selection does not exist-');
		END IF;

		

		v_json := create_etl_json( 'UPDATE', 'DIM_SELECTIONS','{"original_sel_id":"' || p_sel_id || '"}', '{"sel_name":"' || p_name || '","sel_description":"' || p_description || '"}');
		--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end;

	procedure delete_selection(p_sel_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		DELETE FROM selections
		WHERE sel_id = p_sel_id
		RETURNING rowid, sel_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_sel_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'Selection does not exist-');
		END IF;

		

		v_json := create_etl_json( 'DELETE', 'DIM_SELECTIONS','{"original_sel_id":"' || p_sel_id || '"}', null);
		--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end;

	--SELGS_SELS
	procedure add_selection_to_selectiongroup(p_sel_id in number, p_selg_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		v_selg_name				varchar2(50);
		v_sel_name 				varchar2(50);
		v_sel_description		varchar2(1000);

		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;
		BEGIN
			SELECT  selg_name, sel_name, sel_description  INTO  v_selg_name, v_sel_name, v_sel_description FROM (SELECT sg.name as selg_name, s.name as sel_name, s.description as sel_description FROM selection_groups sg, selections s WHERE sg.selg_id = p_selg_id AND s.sel_id = p_sel_id);
			EXCEPTION
				WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20003, 'No Selg ' ||p_selg_id|| ' and No Sel ' ||p_sel_id||' found.');
		END;

		INSERT INTO selgs_sels VALUES (p_selg_id, p_sel_id) RETURNING rowid INTO v_rowid;
		
		v_json := create_etl_json( 'INSERT', 'DIM_SELECTIONS','{"original_sel_id":"' || p_sel_id || '", "original_selg_id":"' || p_selg_id || '"}', '{"selg_name":"' || v_selg_name || '","sel_name":"' || v_sel_name || '","sel_description":"' || v_sel_description || '"}');
		--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end;

	procedure remove_selection_from_selectiongroup(p_sel_id in number, p_selg_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;
		v_changedRow 			number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		DELETE FROM selgs_sels
		WHERE sel_id = p_sel_id AND selg_id = p_selg_id
		RETURNING rowid, sel_id INTO v_rowid, v_changedRow;

		IF v_changedRow != p_sel_id THEN
			RAISE_APPLICATION_ERROR(-20003, 'No Entry of selg '||p_selg_id||' and sel '||p_sel_id||' found.');
		END IF;

		v_json := create_etl_json( 'DELETE', 'DIM_SELECTIONS','{"original_sel_id":"' || p_sel_id || '", "original_selg_id":"' || p_selg_id || '"}', null);
		--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
		log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit; 

	end;

	--ITEMS_SELGS
	procedure add_selectiongroup_to_item(p_selg_id in number, p_item_id in number)
	is
	begin
		INSERT INTO items_selgs VALUES (p_item_id, p_selg_id);
		commit;
	end;

	procedure remove_selectiongroup_from_item(p_selg_id in number, p_item_id in number)
	is
	begin
		DELETE FROM items_selgs
		WHERE item_id = p_item_id AND selg_id = p_selg_id;
		commit;
	end;


END pkg_inventory_management;
/