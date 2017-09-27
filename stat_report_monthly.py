import os,sys,traceback
import MySQLdb
from datetime import datetime,timedelta

__author__ = 'hawk'

now = datetime.now()
this_month_first_day = datetime(now.year, now.month, 1)
last_month_last_day = this_month_first_day - timedelta(1)
last_month_first_day = datetime(last_month_last_day.year, last_month_last_day.month, 1)
start_time = last_month_first_day.strftime("%Y-%m-%d")
end_time = this_month_first_day.strftime("%Y-%m-%d")
report_path = 'stat-report-%s-%s.csv'%(last_month_first_day.strftime("%Y%m%d"), this_month_first_day.strftime("%Y%m%d"))

database_server_host = '10.10.10.82'
database_server_port = 3306
database_name = 'storeshare_prod2'
database_username = 'storeshare_prod2'
database_password = 'storeshare_prod2'

quota_dict = {}
quota_name_dict = {}
show_quota_list = ["2GB", "4GB", "5GB", "10GB", "20GB", "50GB"]
show_client_list = ["web", "iPhone", "ANDROID", "WINDOWS", "MD"]
table_header_list = ['','No. of %s User', 'No. of Active %s User', 'No. of %s New Sign Up User', 'No. of %s Termination', 
'No. of In Web Purchase by %s User', 'Amount of Storage Allocated to %s User', 'Amount of Storage Utilized to %s User']
report_sql_dict = {
	"list_total_user":
		"select quota_id, type, count(*) from SNC_USER where status in (0,1) \
		and quota_id is not null and type in ('STN','SNBB') group by quota_id, type",
	"list_active_user":
		"select a.quota_id, a.type, count(*) from SNC_USER a left join \
		(select distinct operator_id from SNC_LOG where date >= '%s' and date < '%s' ) b \
		on a.id = b.operator_id where (b.operator_id is not null or (a.create_time >= '%s' and a.create_time < '%s')) \
		and status in (0,1) and a.quota_id is not null and a.type in ('STN','SNBB')\
		group by quota_id, type"%(start_time, end_time, start_time, end_time),
	"list_new_user":
		"select b.type, a.current_quota_id, count(*), \
		case when a.channel = 'MD' then 'MD' \
		when POSITION('web/'   IN a.channel) > 0 then 'web' \
		when POSITION('iPhone' IN a.channel) > 0 then 'iPhone' \
		when POSITION('iPad' IN a.channel) > 0 then 'iPhone' \
		when POSITION('ANDROID' IN a.channel) > 0 then 'ANDROID' \
		when POSITION('WINDOWS' IN a.channel) > 0 then 'WINDOWS' else 'unknown' end as client \
		from SNC_STORAGE_UPGRADE a, SNC_USER b \
		where a.user_id = b.id and b.type in ('STN','SNBB') and a.previous_quota_id = 0 and a.previous_quota_value = 0 \
		and a.operation_time >= '%s' and a.operation_time < '%s' group by a.current_quota_id, b.type, client"%(start_time, end_time),
	"list_terminate_user":
		"select quota_id, type, count(*) from SNC_USER where status in (9,10) \
		and modified_time >= '%s' and modified_time < '%s' and quota_id is not null \
		and type in ('STN','SNBB') group by quota_id, type"%(start_time, end_time),
	"list_web_purchase":
		"select a.current_quota_id, b.type, count(*) from SNC_STORAGE_UPGRADE a, SNC_USER b \
		where a.user_id = b.id and b.type in ('STN','SNBB') and a.previous_quota_id > 0 and a.previous_quota_value > 0 \
		and a.operation_time >= '%s' and a.operation_time < '%s' group by b.quota_id, b.type"%(start_time, end_time),
	"list_allocate_storage":
		"select a.quota_id, a.type, round(count(*) * b.max_limit/1024, 2) from SNC_USER a, \
		SNC_STORAGE_TIER b where a.quota_id = b.id and a.status in (0,1) and a.quota_id is not null and \
		a.type in ('STN','SNBB') group by a.quota_id, a.type",
	"list_utilized_storage_file_version":
		"select a.quota_id, a.type, round(sum(b.size)/(1024*1024*1024), 2) from \
		SNC_USER a, SNC_FILE_VERSION b where a.id = b.creator_id and a.status in (0,1) \
		and a.quota_id is not null and a.type in ('STN','SNBB') group by a.quota_id, a.type",
	"list_utilized_storage_convert_file":
		"select a.quota_id, a.type, round(sum(c.size)/(1024*1024*1024), 2) from \
		SNC_FILE_VERSION b \
		inner join SNC_USER a on a.id = b.creator_id \
		inner join SNC_FILE_CONVERT c on c.hash = b.hash \
		where a.status in (0,1) and a.quota_id is not null and a.type in ('STN','SNBB') \
		group by a.quota_id, a.type"
}
insert_sql = "insert into stat_report_monthly(item_id, quota_id, user_type, value, time, channel) values (%s,%s,'%s',%s,'%s','%s')"

def init_quota_dict(conn):
	try:
		cur = conn.cursor()
		cur.execute("select id,name from SNC_STORAGE_TIER")
		quota_list = cur.fetchall()
		for quota in quota_list:
			quota_dict[quota[0]]=quota[1]
			quota_name_dict[quota[1]]=quota[0]
	finally:
		if cur:
			cur.close()

def write_ideas_ldap_table(conn, data_sql, report_type):
	IDEAS_dict = {}
	LDAP_dict = {}
	#get data
	cursor = conn.cursor()
	try:
		cursor.execute(data_sql)
		result = cursor.fetchall()
		IDEAS_dict,LDAP_dict = sort_result_by_user_type(result)
	finally:
		if cursor:
			cursor.close()
	#write data
	if IDEAS_dict and LDAP_dict:
		report_file = open(report_path, 'a+')
		try:
			write_table_head(report_file)
			report_file.write(table_header_list[report_type]%'SingTel Mobile')
			write_dict_line(conn, report_file, IDEAS_dict, report_type, 'IDEAS')
			report_file.write(table_header_list[report_type]%'SingNet')
			write_dict_line(conn, report_file, LDAP_dict, report_type, 'LDAP')
			report_file.write('\n')
		finally:
			report_file.close()
			
def write_client_table(conn, data_sql, report_type):
	IDEAS_dict = {}
	LDAP_dict = {}
	#get data
	cursor = conn.cursor()
	try:
		cursor.execute(data_sql)
		result = cursor.fetchall()
		IDEAS_dict,LDAP_dict = sort_result_by_client(result)
	finally:
		if cursor:
			cursor.close()
	#write data
	if IDEAS_dict and LDAP_dict:
		report_file = open(report_path, 'a+')
		try:
			write_table_head(report_file)
			write_dict_table(conn, report_file, table_header_list[report_type]%'SingTel Mobile', IDEAS_dict, report_type, 'IDEAS')
			write_dict_table(conn, report_file, table_header_list[report_type]%'SingNet', LDAP_dict, report_type, 'LDAP')
			report_file.write('\n')
		finally:
			report_file.close()
			
def write_combine_table(conn, data_sql_list, report_type):
	IDEAS_dict_list = []
	LDAP_dict_list = []
	for data_sql in data_sql_list:
		IDEAS_dict = {}
		LDAP_dict = {}
		#get data
		cursor = conn.cursor()
		try:
			cursor.execute(data_sql)
			result = cursor.fetchall()
			IDEAS_dict,LDAP_dict = sort_result_by_user_type(result)
			IDEAS_dict_list.append(IDEAS_dict)
			LDAP_dict_list.append(LDAP_dict)
		finally:
			if cursor:
				cursor.close()
	IDEAS_combine_dict = {}
	LDAP_combine_dict = {}
	IDEAS_combine_dict, LDAP_combine_dict = combine_dict_sum(IDEAS_dict_list, LDAP_dict_list)
				
	#write data
	if IDEAS_combine_dict and LDAP_combine_dict:
		report_file = open(report_path, 'a+')
		try:
			write_table_head(report_file)
			report_file.write(table_header_list[report_type]%'SingTel Mobile')
			write_dict_line(conn, report_file, IDEAS_combine_dict, report_type, 'IDEAS')
			report_file.write(table_header_list[report_type]%'SingNet')
			write_dict_line(conn, report_file, LDAP_combine_dict, report_type, 'LDAP')
			report_file.write('\n')
		finally:
			report_file.close()

def write_table_head(f):
	for show_quota in show_quota_list:
		f.write(','+show_quota)
	f.write('\n')
	
def write_dict_line(conn, f, line_dict, report_type, user_type, client_type = ''):
	for show_quota in show_quota_list:
		if line_dict.has_key(show_quota):
			value = str(line_dict[show_quota])
			f.write(','+value)
			try:
				cur = conn.cursor()
				_insert_sql = insert_sql % (report_type, quota_name_dict[show_quota], user_type, value, end_time, client_type)
				print _insert_sql
				cur.execute(_insert_sql)
				conn.commit()
			finally:
				if cur:
					cur.close()
		else:
			f.write(',0')
	f.write('\n')

def write_dict_table(conn, f, table_row_title, table_dict, report_type, user_type):
	for show_client in show_client_list:
		if table_dict.has_key(show_client):
			f.write('%s via %s'%(table_row_title, show_client))
			write_dict_line(conn, f, table_dict[show_client], report_type, user_type, show_client)
		else:
			f.write('%s via %s'%(table_row_title, show_client))
			write_dict_line(conn, f, {}, report_type, user_type, show_client)
	f.write('\n')		

def sort_result_by_user_type(result):
	IDEAS_dict = {}
	LDAP_dict = {}
	for line in result:
		_quota_id = line[0]
		_user_type = line[1]
		_number = line[2]
		if _user_type == 'SNBB':
			IDEAS_dict[quota_dict[_quota_id]] = _number
		else:
			LDAP_dict[quota_dict[_quota_id]] = _number
	return IDEAS_dict,LDAP_dict
		
def sort_result_by_client(result):
	IDEAS_dict = {}
	LDAP_dict = {}
	for line in result:
		_user_type = line[0]
		_quota_id = line[1]
		_number = line[2]
		_client = line[3]
		if _user_type == 'SNBB':
			if not IDEAS_dict.has_key(_client):
				IDEAS_dict[_client] = {}
			IDEAS_dict[_client][quota_dict[_quota_id]] = _number
		else:
			if not LDAP_dict.has_key(_client):
				LDAP_dict[_client] = {}
			LDAP_dict[_client][quota_dict[_quota_id]] = _number
	return IDEAS_dict,LDAP_dict
	
def combine_dict_sum(IDEAS_dict_list, LDAP_dict_list):
	IDEAS_combine_dict = {}
	LDAP_combine_dict = {}
	for IDEAS_dict in IDEAS_dict_list:
		for show_quota in IDEAS_dict.keys():
			if IDEAS_combine_dict.has_key(show_quota):
				IDEAS_combine_dict[show_quota] += float(IDEAS_dict[show_quota])
			else:
				IDEAS_combine_dict[show_quota] = float(IDEAS_dict[show_quota])
	for LDAP_dict in LDAP_dict_list:
		for show_quota in LDAP_dict.keys():
			if LDAP_combine_dict.has_key(show_quota):
				LDAP_combine_dict[show_quota] += float(LDAP_dict[show_quota])
			else:
				LDAP_combine_dict[show_quota] = float(LDAP_dict[show_quota])
	return IDEAS_combine_dict, LDAP_combine_dict

def generate_report_table():
	conn = None
	cur = None

	try:
		conn = MySQLdb.connect(host=database_server_host, user=database_username, passwd=database_password, port=database_server_port, db=database_name, charset="utf8")
      
		init_quota_dict(conn)
		write_ideas_ldap_table(conn, report_sql_dict["list_total_user"], 1)		
		write_ideas_ldap_table(conn, report_sql_dict["list_active_user"], 2)
		write_client_table(conn, report_sql_dict["list_new_user"], 3)
		write_ideas_ldap_table(conn, report_sql_dict["list_terminate_user"], 4)
		write_ideas_ldap_table(conn, report_sql_dict["list_web_purchase"], 5)
		write_ideas_ldap_table(conn, report_sql_dict["list_allocate_storage"], 6)
		list_utilized_storage_sql_list = [report_sql_dict["list_utilized_storage_file_version"], report_sql_dict["list_utilized_storage_convert_file"]]
		write_combine_table(conn, list_utilized_storage_sql_list, 7)

	except MySQLdb.Error, e:
		print "Mysql Error %d: %s" % (e.args[0], e.args[1])
	finally:
		if conn:
			conn.close()

	print "generate report table:[%s] finish"%report_path

generate_report_table()