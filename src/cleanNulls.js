var _ = require('underscore');

module.exports =  function cleanNulls (arr) {
	arr.forEach(function(obj) {
		_.each(_.keys(obj), function(key) {
			if (obj[key] === 'NULL' || obj[key] === 'NULL)') {
				obj[key] = '';
			}
		})
	})
}