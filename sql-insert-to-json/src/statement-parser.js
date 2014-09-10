var _ = require('underscore');

//TODO: if this is ever used in production it should be implemented as a proper general solution
//right now the regexs for splitting and sanitizing very ambitiously assume no edge cases

var Parser = function (schema) {

	return new function () {

		this._schema = schema;
		
		this._getTableName = function (statement) {
			var insertSubStr = 'INSERT INTO '
			return statement.substr(insertSubStr.length, statement.indexOf(' VALUES') - insertSubStr.length );
		}

		this._getValues = function(statement) {
			var result,
					valueString,
					insertLength,
					resultArray;


			console.log(statement);
			//TODO: anything unexpected here crashes everything b/c of the [0] array ref
			insertLength = statement.match(/INSERT INTO (")?[a-zA-Z0-9]*(")? VALUES/)[0].length;
			valueString = statement.substr(insertLength + 1, statement.length - insertLength - 2);

			//split by acceptable commas
			resultArray = valueString.split(/,(?=')'|,(?=NULL)|,(?=TRUE)|,(?=FALSE)|,(?=[0-9])/)

			//sanitize each element
			return resultArray.map(function(elem) {
				return elem.replace(/'(?=')/, "||||").replace(/'/g,'').replace('||||',"'");
			});

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