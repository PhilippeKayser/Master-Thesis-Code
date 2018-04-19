create or replace package pkg_customer_management
as
	procedure insert_customer(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date);
	function insert_customer(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date) return number;
	procedure update_customer(p_pers_id in number, p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date);
	procedure delete_customer(p_pers_id in number);

end pkg_customer_management;
/

create or replace package body pkg_customer_management
as
	procedure insert_customer(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date)
	as
		dummy					number;
	begin
		dummy := insert_customer(p_last_name, p_first_name, p_middle_names, p_gender, p_email, p_birthdate);
	end insert_customer;

	function insert_customer(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date) return number
	as
		v_current_timestamp		timestamp;
		v_json					clob;

		--New PERS_ID of the inserted values.
		v_pers_id				number;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		INSERT INTO persons(last_name, first_name, middle_names, gender, email, ptype, birthdate)
		VALUES (p_last_name, p_first_name, p_middle_names, p_gender, p_email, 'C', p_birthdate)
		RETURNING rowid, pers_id INTO v_rowid, v_pers_id;

		v_json := create_etl_json('INSERT', 'DIM_CUSTOMERS', '{"original_pers_id":"' || v_pers_id || '"}', '{"last_name":"' || p_last_name || '", "first_name":"' || p_first_name || '", "middle_names":"' || p_middle_names || '", "gender":"' || p_gender || '", "email":"'||p_email||'", "birthdate":"' ||to_char(p_birthdate,'DD.MM.YYYY')||'"}');
		--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
log_change(v_rowid, 'INSERT',to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

		return v_pers_id;

	end insert_customer;

	procedure update_customer(p_pers_id in number, p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date)
	as
		v_current_timestamp		timestamp;
		v_json					clob;

		--New PERS_ID of the inserted values.
		v_ptype					char;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		SELECT ptype INTO v_ptype FROM persons WHERE pers_id = p_pers_id;

		IF v_ptype != 'C' THEN
			RAISE_APPLICATION_ERROR(-20006, 'Person with ID ' || p_pers_id || ' is not a Customer, but an Employee. Use Employee Management Package to change Data.');
		END IF;

		UPDATE persons
		SET last_name = p_last_name, first_name = p_first_name, middle_names = p_middle_names, gender = p_gender, email = p_email, birthdate = p_birthdate
		WHERE pers_id = p_pers_id
		RETURNING rowid INTO v_rowid;

		v_json := create_etl_json('UPDATE', 'DIM_CUSTOMERS', '{"original_pers_id":"' || p_pers_id || '"}', '{"last_name":"' || p_last_name || '", "first_name":"' || p_first_name || '", "middle_names":"' || p_middle_names || '", "gender":"' || p_gender || '", "email":"'||p_email||'", "birthdate":"' ||to_char(p_birthdate,'DD.MM.YYYY')||'"}');
		--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
log_change(v_rowid, 'UPDATE',to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end update_customer;

	procedure delete_customer(p_pers_id in number)
	as
		v_current_timestamp		timestamp;
		v_json					clob;

		--New PERS_ID of the inserted values.
		v_ptype					char;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;
		
		SELECT ptype INTO v_ptype FROM persons WHERE pers_id = p_pers_id;

		IF v_ptype != 'C' THEN
			RAISE_APPLICATION_ERROR(-20006, 'Person with ID ' || p_pers_id || ' is not a Customer, but an Employee. Use Employee Management Package to change Data.');
		END IF;

		DELETE FROM persons
		WHERE pers_id = p_pers_id
		RETURNING rowid INTO v_rowid;

		--v_json := create_etl_json('DELETE', 'DIM_CUSTOMERS', '{"original_pers_id":"' || p_pers_id || '"}', null);
		--write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
		--log_change(v_rowid, 'DELETE',to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

	end delete_customer;

end pkg_customer_management;
/