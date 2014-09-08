#SQL Insert Statements to JSON

This module takes an array of SQL Insert statements and converts them to JSON objects. For instance:

	var InsertParser = require('sql-insert-to-json'),
		statements = [
			"INSERT INTO TEST VALUES(1,'foo',NULL)",
			"INSERT INTO TEST VALUES(NULL,2,'bar')",
		],
		parser = new InsertParser;

	parser.parse(statements);

	//returns [ [ '1','foo','NULL' ], [ 'NULL', '2', 'bar' ] ]

With a schema:

	var InsertParser = require('sql-insert-to-json'), 
		schema = ['value1', 'value2', 'value3'],
		statements = [
			"INSERT INTO TEST VALUES(1,'foo',NULL)",
			"INSERT INTO TEST VALUES(NULL,2,'bar')",
		],
		parser = new InsertParser(schema);

	parser.parse(statements);

	//returns [ 
	//	{value1: '1', value2: 'foo', value3: 'NULL' }, 
	//	{value1: 'NULL', value2: '2', value3: 'bar' } 
	//]