var test = require('tape');

//lib
var Parser = require('./../src/statement-parser.js');

var valueTest;

//test for parsing of values to be run multiple times with different input statements
valueTest = function (testName, statement) {

		test ( testName, function(t) {

			// var statement = "INSERT INTO CODEDLOCATION VALUES(20,22,'Central','Narayani','Chitawan','Bharatpur',NULL,NULL,'Saptagandaki Multiple Campus Bharatpur',NULL)";
			var parser = new Parser();
			var result;

			t.plan(3);

			result = parser._getValues(statement);

			t.equals(typeof result, 'object', 'Result is an object (array)');
			t.equals(result.length, 10, 'Array is the expected length');
			t.equals(result[2], 'Central', 'Third element of Array has the expected value');

		});

}

//==========TESTS==========

test( 'Parser can parse table name from statement.', function (t) {

	var statement = "INSERT INTO CODEDLOCATION VALUES(20,22,'Central','Narayani','Chitawan','Bharatpur',NULL,NULL,'Saptagandaki Multiple Campus Bharatpur',NULL)";
	var parser = new Parser();

	t.plan(1);

	t.equals( parser._getTableName(statement), 'CODEDLOCATION', 'Table name is the expected value.');

});

//-----------------------------------------------------

valueTest('Parser can parse values from statement.', "INSERT INTO CODEDLOCATION VALUES(20,22,'Central','Narayani','Chitawan','Bharatpur',NULL,NULL,'Saptagandaki Multiple Campus Bharatpur',NULL)");

//-----------------------------------------------------

valueTest('Parser can parse values from statement with a comma.', "INSERT INTO CODEDLOCATION VALUES(20,22,'Central','Narayani','Chitawan','Bharatpur',NULL,NULL,'Saptagandaki, Multiple Campus Bharatpur',NULL)");

//-----------------------------------------------------

test ('Parser converts escaped quotes to single quotes in values', function(t) {
	
	var statement = "INSERT INTO FOO VALUES('bar''s the best')",
			parser = new Parser(),
			result;

	t.plan(1);

	result = parser._getValues(statement)[0];

	t.equals( result , 'bar\'s the best', 'Escaped quote is properly converted.');

})

//-----------------------------------------------------

test ('Parser can parse an array of 3 equal-length schemaless statements into an array of equal-length arrays', function(t) {
	
	var statements = [
				"INSERT INTO TENDERNOTICE VALUES(4,'14/ 070/ 071','Current','http://eproc.dor.gov.np/tender_details.php?tid=31866','A. Earthworks in Bokhim Iwa Hwaku road B. Earthworks in Majhitar Tamanggaun Bokhim Hoda Tundikhel Hwaku dovan Bazar road/DRODKT/107/070/071-55*','Issued by\u0009EDRO No. 4, Dhankuta','2014-04-30','Fast Daily','2014-05-15','2014-05-16','2014-05-16',NULL,'DRODKT/107/070/071-55*/A. Earthworks in Bokhim Iwa Hwaku road B. Earthworks in Majhitar Tamanggaun Bokhim Hoda Tundikhel Hwaku dovan Bazar road',NULL,NULL,NULL)",
				"INSERT INTO TENDERNOTICE VALUES(5,'14/ 070/ 071','Current','http://eproc.dor.gov.np/tender_details.php?tid=31891','Upgrading works in Bhojpur Urban road/DRODKT/148/070/071-67*','Issued by\u0009EDRO No. 4, Dhankuta','2014-04-30','Fast Daily','2014-05-15','2014-05-16','2014-05-16',NULL,'DRODKT/148/070/071-67*/Upgrading works in Bhojpur Urban road',NULL,NULL,NULL)",
				"INSERT INTO TENDERNOTICE VALUES(6,'14/ 070/ 071','Current','http://eproc.dor.gov.np/tender_details.php?tid=31909','Upgrading works in Chirkhuwa Dingla Kulung road/DRODKT/148/070/071-69*','Issued by\u0009EDRO No. 4, Dhankuta','2014-04-30','Fast Daily','2014-05-15','2014-05-16','2014-05-16',NULL,'Upgrading works in Chirkhuwa Dingla Kulung road/DRODKT/148/070/071-69*',NULL,NULL,NULL)"
			],
			parser = new Parser(),
			result;

	t.plan(2);

	result = parser.parse(statements);

	t.equals( result.TENDERNOTICE.length, 3, 'Result is a length 3 array');
	t.equals( result.TENDERNOTICE[0].length === result.TENDERNOTICE[1].length && result.TENDERNOTICE[1].length === result.TENDERNOTICE[2].length, true, 'Each array is the same length'); 

})

//-----------------------------------------------------

test ('Parser can parse a statement into a schema', function (t) {

	var statements = [
				"INSERT INTO TEST VALUES(1,'Crazy',NULL)",
				"INSERT INTO TEST VALUES(NULL,2,'Yup')",
			],

			schema = ['Value1', 'Value2', 'Value3'],

			parser = new Parser(schema),

			expectedResult = { 
				TEST: [
					{
						Value1: '1',
						Value2: 'Crazy',
						Value3: 'NULL'
					},
					{
						Value1: 'NULL',
						Value2: '2',
						Value3: 'Yup'
					}
				]
			},

			result;

	t.plan(2);

	result = parser.parse(statements);

	t.equals(result.TEST && Array.isArray(result.TEST) && result.TEST.length === 2, true, 'The result is a length 2 array');
	t.deepEquals(result, expectedResult, 'The statements are parsed into correct JSON objects.')

});
