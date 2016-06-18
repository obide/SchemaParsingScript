#! /usr/bin/env python

#Portrays all table attributes of a specified schema
#Check for Table Name, Type, Incremental, Query, Key, Columns with a Key
#
#TO DO: Add Column specifying data source / data file
#
# June 17, 2016 by 
# NADIM SARRAS
# ANAHIT SARAO


import sys, incorta, json as json, re as regex, xml.etree.ElementTree as xml; 

DEBUG = False #Enable or disbale debug print statements
SEPERATOR = "------------------------------------------------------------------------------------------------------------------------------"

"""
Function takes table input and parses table for pattern matching XML
	args:
		session(string): The session to incorta
		schema(xml.etree.ElementTree): The schema object
	returns: 
		Nothing
	prints:
		Prints out the table with Table Name, Type, Incremental, Query, Key, Columns with a Key
"""
def display_schema(session, schema):
	r = incorta.get(session, "/service/schema/getSchema", {"schemaId": schema["id"]})
	schema_xml = xml.fromstring(r.content)
	table_to_key_columns = which_table_has_key(schema_xml)
	key_val = check_table_key(schema_xml)
	for table in schema_xml.findall(".//datasets/*"):
		inc = "incremental" in table.attrib and "true" == str(table.attrib["incremental"]).lower()
		query_xml = table.find("queryUpdate")
		correct_query = Q_REGEX.match(xml.tostring(query_xml)) if not query_xml is None else False
		table_name = table.attrib['table']
		table_has_key = table_to_key_columns.has_key(table_name.split('.')[1])
		print ("{:<50} {:<10} {:<15} {:<20} {:<10} {:<20}" ).format(table_name, table.tag, \
			"Yes" if inc else "No", "-" if not inc else "Yes" if correct_query else "No",
			"Yes" if key_val[table_name.split('.')[1]] else "No", str(table_to_key_columns[table_name.split('.')[1]]) if table_has_key else "-")

"""
Parses through all tables within the schema. Within every table, all columns are traversed through 
and checked to see if they contain a key. If so, the name of the column is appended to a dictionary
which is later returned. 
"""
def which_table_has_key(schema_xml):
	columns_with_key = {} 
	
	try:
		tables = schema_xml.find('schemaData').find('schema').find('tables')
		tables = tables.findall('table')
	except Exception, e:
		return columns_with_key
	for table in tables:
		column_list = []
		for column in table.iter():
			if column.get('function') == 'key':
				column_list.append(column.get('name'))
		add_column_to_table_list(columns_with_key, table.get('name'), column_list)
	return columns_with_key

"""
Parses through all columns within all tables. Checks if any columns have a key, if none then the
key_check boolean is set to false. If any of the columns have a key the key_check boolean is set
to true. Boolean value is then appened to a dictionary. Dictionary is then returned.
"""

def check_table_key(schema_xml):

	check_key = {}
	try:
		tables = schema_xml.find('schemaData').find('schema').find('tables')
		tables = tables.findall('table')
	except Exception, e:
		return check_key
	for table in tables:
		key_check = False
		for column in table.iter():
			if column.get('function') == 'key':
				key_check = True			
		add_bool_to_key_list(check_key, table.get('name'), key_check)
	return check_key

"""
Add column_list as a single element to the columns_with_key dictionary. Column_list is mapped
as a value to the correlating key of the dictionary, i.e. table_name 
"""
def add_column_to_table_list(table_to_columns_dict, table_name, column_with_key_name):


	if table_to_columns_dict.has_key('table_name'):
		table_to_columns_dict[table_name] = table_to_columns_dict[table_name].append(column_with_key_name)
	else:
		table_to_columns_dict[table_name] = [column_with_key_name]
"""
Add key_boolean as a single element to the columns_with_key dictionary. Boolean of key is mapped
as a value to the correlating key of the dictionary, i.e. table_name  
"""

def add_bool_to_key_list(table_to_columns_dict, table_name, key_bool_name):

	if table_to_columns_dict.has_key('table_name'):
		table_to_columns_dict[table_name] = table_to_columns_dict[table_name].append(key_bool_name)
	else:
		table_to_columns_dict[table_name] = key_bool_name



"""
Allows pattern matching for schema, print out header statement for print output
	args:
		session(string): The session to incorta
		schema_list(xml.etree.ElementTree): The schema object
		schema_pattern(): Pattern of XML parsed schema
	returns: 
		Nothing
	prints:
		Has top level header print statement
"""

def list_schemas(session, schema_list, schema_pattern):
	schema_found = False
	#loops through schema list and displays output
	for schema in schema_list:
		schema_name = schema["name"]
		if schema_pattern.match(schema_name) :
			print " Schema ", schema_name
			print (SEPERATOR)
			print ("{:<50} {:<10} {:<15} {:<20} {:<10} {:<20}").format("Table name", "Type", \
				"Incremental", "Query has (?)", "Key?", "Key Column Names")
			print (SEPERATOR)
			schema_found = True
			display_schema(session, schema)
			print (SEPERATOR+"\n")
	if not schema_found:
		print ("WARNNING: Could not find schemas matching your criteria, please "
		"try '%' if you want to include all schemas.")

if len(sys.argv) < 5 :
	print ("Invalid arguments are: server tenant user pass schema_pattern")
	print ("example:")
	print ("\t http://localhost:8080/incorta demo super@incorta.com password EBS_%")

else :
	args = sys.argv[1:]
	server = args[0] #Arugument for Server 
	tenant = args[1] #Arugument for Tenant
	user = args[2] #Arugument for User
	password = args[3] #Arugument for Password 
	schema_pattern = regex.compile(str(args[4]).replace("%", ".*"))
	Q_REGEX = regex.compile("[^\\?]*\\?[^\\?]*")
	
	session = incorta.login(server, tenant, user, password)
	
	r = incorta.get(session, "/service/schema/getSchemas")
	try :
		list_schemas(session, json.loads(r.content)['schemas'], schema_pattern)
	finally :
		incorta.logout(session)