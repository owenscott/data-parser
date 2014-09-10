var createHash = require('./createMD5'),
	_ = require('underscore');

module.exports = function createSupplierHash (supplierObj) {
	
	var allFields =  _.pick(supplierObj, 'SUPPLIER_NAME', 'SUPPLIER_REGISTRATION_CODE', 'SUPPLIER_REGISTRATION_CODE_SOURCE', 'SUPPLIER_COUNTRY')
	var hash = {};

	hash.complete = createHash(allFields);

	if (supplierObj.SUPPLIER_REGISTRATION_CODE && supplierObj.SUPPLIER_REGISTRATION_CODE_SOURCE) {
		var codeOnly = _.pick(supplierObj, ['SUPPLIER_REGISTRATION_CODE', 'SUPPLIER_REGISTRATION_CODE_SOURCE'])
		hash.code = createHash(codeOnly);
	}
	else {
		hash.code = '';
	}



	return hash;



}


