var fs = require('fs');
var jsonData = JSON.parse(fs.readFileSync('./2014-09-15 Release 2 (QA\'d).json').toString())
var output = []; 
jsonData.forEach(function(tender) { 
	if (tender.locations) {
		tender.locations.forEach(function(loc) { 
			var temp = {}; 
			Object.keys(loc).forEach(function(key) {
				temp[key] = loc[key]; 
			});
			temp['id'] = tender.id; 
			output.push(temp) 
		}) 
	}

})

fs.writeFileSync('./locations.json', JSON.stringify(output));