var crypto = require('crypto'),
	_ = require('underscore');

module.exports = function createHash(obj) {

	var hashString = '',
		md5 = crypto.createHash('md5');

	_.keys(obj).forEach(function(key) {
		hashString += obj[key];
	});

	md5.update(hashString);

	return md5.digest('hex');
	
}