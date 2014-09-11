var _ = require('underscore');

//TODO: if this is ever used in production it should be implemented as a proper general solution
//right now the regexs for splitting and sanitizing very ambitiously assume no edge cases

var Parser = function (schema) {

	return new function () {

		this._schema = schema;
		
		this._getTableName = function (statement) {
			var insertSubStr = 'INSERT INTO '
			return statement.substr(insertSubStr.length, statement.indexOf(' VALUES') - insertSubStr.length ).replace(/"/g, '');
		}

		this._getValues = function(statement) {
			var result,
					valueString,
					insertLength,
					resultArray;

			//TODO: anything unexpected here crashes everything b/c of the [0] array ref
			insertLength = statement.match(/INSERT INTO (")?[a-zA-Z0-9]*(")? VALUES/)[0].length;
			valueString = statement.substr(insertLength + 1, statement.length - insertLength - 2);

			//split by acceptable commas
			resultArray = valueString.split(/,(?=')'|,(?=NULL)|,(?=TRUE)|,(?=FALSE)|,(?=[0-9])/)
			// resultArray = valueString.split(/,(?=')'|,(?=NULL)|,(?=TRUE)|,(?=FALSE)|,(?:[^0-9])(?=[0-9])/);

			//sanitize each element
			resultArray = resultArray.map(function(elem) {
				return elem.replace(/'(?=')/, "||||").replace(/'/g,'').replace('||||',"'");
			});

			//total monkey patch :(
			// console.log('9',resultArray[9]);
			// console.log('10',resultArray[10])
			if (this._getTableName(statement) === 'AWARD')  {
				if (resultArray[9].substr(0,3) === 'Rs.') {
					var i = 9;
					while (i) {

						if (resultArray[i].substr(0,4) !== 'http') {
							i = i + 1;

						}
						else {
							break;
						}						
					}
					// console.log(i);
					// console.log(resultArray)
					resultArray[9] = resultArray.slice(9,i).join('');
					resultArray = resultArray.slice(0,10).concat(resultArray.slice(i,resultArray.length))
			
				}
				

				if (resultArray.length !== 16) {
					console.log('ERROR WITH RESULT ARRAY')
					console.log(resultArray);
					console.log(resultArray.length)
					process.exit();
				}

				// INSERT INTO AWARD VALUES(58,NULL,NULL,NULL,'N/A','N/A',NULL,NULL,NULL,'Rs. 0.00','http://www.edolidar.gov.np/tender_details.php?tid=52777','3/2070/71','Construction of Jhaukhel 6,2 & 1 to Changu Road','Issued by : DDC Bhaktapur','2014-03-31','N/A')


			}	


			return resultArray;


		}

		this.parse = function(statements) {


			var result,
					resultArr,
					resultObj = {},
					i,
					tableName;

			result = {};

			if (statements) {
				for (i in statements) {

					tableName = this._getTableName(statements[i]);
					result[tableName] = result[tableName] || [];
					resultArr = this._getValues(statements[i]);
					
					resultObj = {};
					
					if (this._schema) {
						for (j in resultArr) {
							resultObj[this._schema[tableName][j]] = resultArr[j];
						}
						result[tableName].push(_.clone(resultObj));
					}
					else {
						result[tableName].push(resultArr.slice(0));
					}
				}
			}
			

			return result;

		}

	}
}

module.exports = Parser;