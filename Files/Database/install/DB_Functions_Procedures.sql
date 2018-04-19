--DEFINITION OF HELPER FUNCTIONS
create or replace procedure write_json_to_file(json in clob, p_filename in varchar2, cur_timestamp in TIMESTAMP)
is
	fHandle			UTL_FILE.FILE_TYPE;
	json_obj 		JSON_OBJECT_T;
begin

    json_obj := new JSON_OBJECT_T(json);
    json_obj.put('timestamp', to_char(cur_timestamp,'YYYY.MM.DD HH24:MI:SS'));

	fHandle := UTL_FILE.FOPEN('ETL_DIRECTORY_AWAITING', p_filename || '.json', 'w');



	UTL_FILE.PUT_LINE(fHandle, json_obj.to_clob());

	UTL_FILE.FCLOSE(fHandle);


	EXCEPTION
	  WHEN OTHERS THEN
	    DBMS_OUTPUT.PUT_LINE('Exception: SQLCODE=' || SQLCODE || '  SQLERRM=' || SQLERRM);
    
end write_json_to_file;
/

create or replace function create_etl_json(p_action in varchar2, p_targetDimension in varchar2, p_primaryKeys in clob, p_values in clob) return clob
is
	json_obj 		JSON_OBJECT_T;
	json_pKeys 		JSON_OBJECT_T;
	json_values		JSON_OBJECT_T;
begin
	json_obj := new JSON_OBJECT_T();

	json_obj.put('action', p_action);
	json_obj.put('targetDimension', p_targetDimension);

	json_pKeys := new JSON_OBJECT_T(p_primaryKeys);
	json_obj.put('primaryKey',json_pKeys);
	
	if p_values is not null then
		json_values := new JSON_OBJECT_T(p_values);
		json_obj.put('values',json_values);
	end if;
	
	return json_obj.to_clob();
end create_etl_json;
/


create or replace procedure log_change(p_row_id in ROWID, p_action in varchar2, p_filename in VARCHAR2, p_content in clob)
is
	v_rowid ROWID;
	v_action varchar2(6);
begin
	begin
		SELECT rowid, action INTO v_rowid, v_action FROM awaiting_table WHERE current_rowid = p_row_id;

		Exception
			when no_data_found then v_rowid := chartorowid(0);
	end;

	if v_rowid = chartorowid(0) then
		insert into awaiting_table VALUES (p_row_id, p_action, p_filename, p_content);
	else
		if (v_action = 'INSERT' or v_action = 'UPDATE') and p_action ='UPDATE' then
			--Update Entry
			UPDATE awaiting_table
			SET content = p_content, filename = p_filename
			WHERE current_rowid = v_rowid;
		elsif v_action = 'INSERT' AND p_action = 'DELETE' then
			--remove element completely
			DELETE FROM awaiting_table
			WHERE rowid = v_rowid;
		elsif v_action = 'UPDATE' AND p_action = 'DELETE' then
			--überschreibe
			UPDATE awaiting_table
			SET filename = p_filename, action = p_action, content = p_content
			WHERE current_rowid = v_rowid;
		end if;
	end if;
end;
/

--Procedure von http://www.astral-consultancy.co.uk/cgi-bin/hunbug/doco.cgi?11070
CREATE OR REPLACE PROCEDURE dpr_clobToFile(p_fileName IN VARCHAR2, p_clob IN CLOB) 
IS
  c_amount         CONSTANT BINARY_INTEGER := 32767;
  l_buffer         VARCHAR2(32767);
  l_chr10          PLS_INTEGER;
  l_clobLen        PLS_INTEGER;
  l_fHandler       UTL_FILE.FILE_TYPE;
  l_pos            PLS_INTEGER    := 1;

BEGIN

  l_clobLen  := DBMS_LOB.GETLENGTH(p_clob);
  l_fHandler := UTL_FILE.FOPEN('JSON_MENU_DIRECTORY', p_fileName,'W', c_amount);

  WHILE l_pos < l_clobLen LOOP
    l_buffer := DBMS_LOB.SUBSTR(p_clob, c_amount, l_pos);     
    EXIT WHEN l_buffer IS NULL;
    l_chr10  := INSTR(l_buffer,CHR(10),-1);
    IF l_chr10 != 0 THEN
      l_buffer := SUBSTR(l_buffer,1,l_chr10-1);
    END IF;
    UTL_FILE.PUT_LINE(l_fHandler, l_buffer,TRUE);
    l_pos := l_pos + LEAST(LENGTH(l_buffer)+1,c_amount);
  END LOOP;

  UTL_FILE.FCLOSE(l_fHandler);

EXCEPTION
WHEN OTHERS THEN
  IF UTL_FILE.IS_OPEN(l_fHandler) THEN
    UTL_FILE.FCLOSE(l_fHandler);
  END IF;
  RAISE;

END;
/ 