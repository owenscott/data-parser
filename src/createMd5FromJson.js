var crypto = require('crypto');




module.exports = function(obj) {

	var md5 = crypto.createHash('md5');
	md5.update(obj)
	return md5.digest('hex');

}