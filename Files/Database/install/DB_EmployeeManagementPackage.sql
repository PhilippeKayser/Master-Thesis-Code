create or replace package pkg_employee_management
as
	procedure insert_employee(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date, p_manager_id in number, p_job_id in number, p_ssec in varchar2, p_salary in number);
	function insert_employee(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date, p_manager_id in number, p_job_id in number, p_ssec in varchar2, p_salary in number) return number;
	procedure update_employee(p_emp_id in number, p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date, p_manager_id in number, p_job_id in number, p_ssec in varchar2, p_salary in number);
	procedure delete_employee(p_emp_id in number);

end pkg_employee_management;
/

create or replace package body pkg_employee_management
as
	procedure insert_employee(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date, p_manager_id in number, p_job_id in number, p_ssec in varchar2, p_salary in number)
	as
		dummy					number;
	begin
		dummy := insert_employee(p_last_name, p_first_name, p_middle_names, p_gender, p_email, p_birthdate, p_manager_id, p_job_id, p_ssec, p_salary);
	end insert_employee;

	function insert_employee(p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date, p_manager_id in number, p_job_id in number, p_ssec in varchar2, p_salary in number) return number
	as
		v_current_timestamp		timestamp;
		v_json					clob;

		--New PERS_ID of the inserted values.
		v_emp_id				number;
		v_job_title				varchar2(50);
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		begin
			SELECT job_title INTO v_job_title FROM jobs WHERE job_id = p_job_id;
			EXCEPTION
				WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20007, 'Job doesnt exist');
		end;

		INSERT INTO persons(last_name, first_name, middle_names, gender, email, ptype, birthdate)
		VALUES (p_last_name, p_first_name, p_middle_names, p_gender, p_email, 'E', p_birthdate)
		RETURNING rowid, pers_id INTO v_rowid, v_emp_id;

		INSERT INTO employee_details(emp_id, manager_id, job_id, social_sec_num, salary)
		VALUES (v_emp_id, p_manager_id, p_job_id, p_ssec, p_salary);



		v_json := create_etl_json( 'INSERT', 'DIM_EMPLOYEES', '{"original_emp_id":"' || v_emp_id || '"}', '{"last_name":"' || p_last_name || '", "first_name":"' || p_first_name || '", "middle_names":"' || p_middle_names || '", "gender":"' || p_gender || '", "birthdate":"' ||to_char(p_birthdate,'DD.MM.YYYY')||'","original_job_id":"' || p_job_id || '","job_title":"' || v_job_title || '"}');
		--write_json_to_file(v_json, 'INSERT', v_current_timestamp, false);
		log_change(v_rowid,'INSERT', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;

		return v_emp_id;
	end insert_employee;


	procedure update_employee(p_emp_id in number, p_last_name in varchar2, p_first_name in varchar2, p_middle_names in varchar2, p_gender in char, p_email in varchar2, p_birthdate in date, p_manager_id in number, p_job_id in number, p_ssec in varchar2, p_salary in number)
	as
		v_current_timestamp		timestamp;
		v_json					clob;

		--New PERS_ID of the inserted values.
		v_emp_id				number;
		v_job_title				varchar2(50);
		v_ptype					char;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		SELECT ptype INTO v_ptype FROM persons WHERE pers_id = p_emp_id;

		IF v_ptype != 'E' THEN
			RAISE_APPLICATION_ERROR(-20006, 'Person with ID ' || p_emp_id || ' is not an Employee, but a Customer. Use Customer Management Package to change Data.');
		END IF;

		begin
			SELECT job_title INTO v_job_title FROM jobs WHERE job_id = p_job_id;
			EXCEPTION
				WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(-20007, 'Job doesnt exist');
		end;
    
		UPDATE persons
		SET last_name = p_last_name, first_name = p_first_name, middle_names = p_middle_names, gender = p_gender, email = p_email, birthdate = p_birthdate
		WHERE pers_id = p_emp_id
		RETURNING rowid INTO v_rowid;

		UPDATE employee_details
		SET manager_id = p_manager_id, job_id = p_job_id, social_sec_num = p_ssec, salary = p_salary
        WHERE emp_id = p_emp_id;

		v_json := create_etl_json( 'UPDATE', 'DIM_EMPLOYEES', '{"original_emp_id":"' || p_emp_id || '"}', '{"last_name":"' || p_last_name || '", "first_name":"' || p_first_name || '", "middle_names":"' || p_middle_names || '", "gender":"' || p_gender || '", "birthdate":"' ||to_char(p_birthdate,'DD.MM.YYYY')||'","original_job_id":"' || p_job_id || '","job_title":"' || v_job_title || '"}');
		--write_json_to_file(v_json, 'UPDATE', v_current_timestamp, false);
log_change(v_rowid,'UPDATE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end update_employee;

	procedure delete_employee(p_emp_id in number)
	is
		v_current_timestamp		timestamp;
		v_json					clob;

		--New PERS_ID of the inserted values.
		v_ptype					char;
		v_rowid ROWID;
	begin
		v_current_timestamp := systimestamp;

		SELECT ptype INTO v_ptype FROM persons WHERE pers_id = p_emp_id;

		IF v_ptype != 'E' THEN
			RAISE_APPLICATION_ERROR(-20006, 'Person with ID ' || p_emp_id || ' is not an Employee, but a Customer. Use Customer Management Package to change Data.');
		END IF;


		DELETE FROM persons
		WHERE pers_id = p_emp_id
		RETURNING rowid INTO v_rowid;

		v_json := create_etl_json( 'DELETE', 'DIM_EMPLOYEES', '{"original_emp_id":"' || p_emp_id || '"}', null);
--		write_json_to_file(v_json, 'DELETE', v_current_timestamp, false);
log_change(v_rowid,'DELETE', to_char(v_current_timestamp, 'YYYYMMDDHHMISSFF4'), v_json);
		commit;
	end;
end;
/