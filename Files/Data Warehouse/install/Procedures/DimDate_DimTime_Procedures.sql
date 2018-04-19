create or replace procedure fill_dim_date(from_date in varchar2, end_date in varchar2) as
	v_current_date date;
	final_date date;
begin

	v_current_date := to_date(from_date, 'dd.MM.yyyy');
	final_date := to_date(end_date, 'dd.MM.yyyy');

	while v_current_date <= final_date
	loop
		dbms_output.put_line('Date is ' || v_current_date);
		insert into dim_date (
			year,
			halfyear,
			quarter,
			month,
			monthname,
			day,
			week,
			weekday,
			weekdayname,
			dayofyear
		) values (
			extract(year from v_current_date),
			round(to_number(to_char(v_current_date, 'Q'))/2),
			to_number(to_char(v_current_date, 'Q')),
			extract(month from v_current_date),
			to_char(v_current_date, 'MONTH'),
			extract(day from v_current_date),
			to_number(to_char(v_current_date, 'IW')),
			to_number(to_char(v_current_date, 'D')),
			to_char(v_current_date, 'DAY'),
			to_number(to_char(v_current_date, 'DDD'))
		);

		v_current_date := v_current_date+1;
	end loop;
end;
/
create or replace procedure fill_dim_time as
	v_current_hour number;
	v_current_minute number;
begin
	v_current_hour := 0;
	v_current_minute := 0;

	while v_current_hour < 24
	loop
		while v_current_minute < 60
		loop
            dbms_output.put_line(v_current_hour || ':' || v_current_minute);
			insert into dim_time(hour,minute) values (v_current_hour, v_current_minute);	
            v_current_minute := v_current_minute + 1;
		end loop;
        v_current_minute := 0;
		v_current_hour := v_current_hour+1;
	end loop;
end;
/