var fs = require('fs'),
	data,
	i,
	locationRef,
	gadmRaw = [],
	gadm = [],
	results = [],
	adm1Ref, 
	adm2Ref,
	adm3Ref,
	adm4Ref,
	_ = require('underscore'),
	csvParse = require('csv-parse'),
	async = require('async'),
	csvGenerate = require('csv-generate'),
	csvStringify = require('csv-stringify');

locationRef = JSON.parse(fs.readFileSync('./ref/locations.json').toString());
data = JSON.parse(fs.readFileSync('./output/output.json').toString());

for (i in data ) {

	// var desiredFields = ['TENDERNOTICENUMBER','CONTRACTNUMBER','CONTRACTTYPE','PROJECTNUMBER','PROJECTFUNDER',/*'CONTRACTNAME','CONTRACTDESCRIPTION',*/'COSTESTIMATE'],
	var desiredFields = ['ID','TENDERNOTICENUMBER','STATUS','URL','CONTRACTDETAILS','ISSUER','PUBLICATIONDATE','PUBLISHEDIN','DOCUMENTPURCHASEDEADLINE','SUBMISSIONDEADLINE','OPENINGDATE','CONTRACTNAME','CONTRACTDESCRIPTION','COSTESTIMATE','ESTIMATECURRENCY','DATASOURCE','TENDER_NOTICE_ID','CODER_ID','CONTRACTNUMBER','CONTRACTTYPE','PROJECTNAME','PROJECTNUMBER','PROJECTFUNDER'],
		tempD = {},
		field,
		gadm = [],
		d,
		j,
		locationObj,
		location;

	tempD.locations = [];	

	d = data[i];

	for (j in desiredFields) {

		field = desiredFields[j];

		if (d.keyValuePairs.merge[field].match) {
			tempD[field] = d.keyValuePairs.merge[field].value;
		}
		else if (d.keyValuePairs.originals[0][field]) {
			tempD[field] = d.keyValuePairs.originals[0][field];
		}
		else if (d.keyValuePairs.originals[1][field]) {
			tempD[field] = d.keyValuePairs.originals[1][field];
		}
		else {
			tempD[field] = '';
		}

	}

	if (data[i]['CONTRACTNUMBER'] === 'DORW/RBT/04/2070/071DORW/RBT/05/2070/071') {
		console.log(data[i]);
	}


	for (j in d.arrays.merge.locations) {
		locationObj = {};
		location = d.arrays.merge.locations[j].value.split('|');
		locationObj = stepThroughLocations(locationRef.adm1[location[0]] , _.clone(location))
		if (locationObj) {
			tempD.locations.push(JSON.stringify(_.clone(locationObj)));
		}
	}

	var otherLocations = d.arrays.originals.locations[0].concat(d.arrays.originals.locations[1]);

	otherLocations.forEach( function(location) {
		var locationObj;
		location = location.split('|');
		locationObj = stepThroughLocations(locationRef.adm1[location[0]] , _.clone(location))

		if (locationObj) {
			tempD.locations.push(JSON.stringify(_.clone(locationObj)));
		}
	})


	tempD.locations = _.uniq(tempD.locations);

	results.push(_.clone(tempD));
}

gadmRaw[0] = fs.readFileSync('./ref/gadm-adm1.csv').toString();
gadmRaw[1] = fs.readFileSync('./ref/gadm-adm2.csv').toString();
gadmRaw[2] = fs.readFileSync('./ref/gadm-adm3.csv').toString();
gadmRaw[3] = fs.readFileSync('./ref/gadm-adm4.csv').toString();

i = 0;


//read gadm data and convert to a better format
async.eachSeries(gadmRaw, function(csv, callback) {
	csvParse(csv, {columns:true}, function(err, data) {
		gadm[i] = data;
		i++;
		callback();
	})
},
	function(){

		var csvOutput = [],
		csvText = '';

		//clean up gadm data
		for (i in gadm) {
			gadm[i] = _.map(gadm[i], function(record) {
				var level = parseInt(i) + 1;
				return {
					level: 'adm' + level,
					name: record['NAME_' + level],
					pid: record.PID,
					type: record['ENGTYPE_' + level],
					levelId: record['ID_' + level]
				}
			});
		}
		//add gadm data to locations and convert to flat csv output
		results.forEach(function(result) {


			//add gadm data
			result.locations = _.map(result.locations, function(location) {
				location = JSON.parse(location);
				var level = parseInt(location.level.substr(3,1)) - 1,
					name = location.name;
				location = 	_.where(gadm[level], {name:name})[0];
				if(location) {
					return location;
				}
				
			})
			//convert to flat csv format
			result.locations.forEach(function(location) {
				csvObj = _.pick(result, desiredFields);
				csvObj = _.extend(csvObj, location);
				csvOutput.push(_.clone(csvObj));                                   
			})			


		})

		csvText = csvText + _.keys(csvOutput[0]).join(',') + '\r\n';
		
		csvOutput.forEach(function(result) {

			var arr;
			
			arr = _.map( _.values(result), function(val) {
				val = val.replace(/,/,'');
				val = val.replace(/"/,'');
				return val;
			})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
			csvText = csvText + arr.join(',');
			csvText = csvText + '\r\n';
		});

		fs.writeFileSync('./output/output-with-locations.json', JSON.stringify(results));
		fs.writeFileSync('./output/output-with-locations.csv', csvText);

	}
);

  // { PID: '2058',
  //   ID_0: '157',
  //   ISO: 'NPL',
  //   NAME_0: 'Nepal',
  //   ID_1: '4',
  //   NAME_1: 'Mid-Western',
  //   NL_NAME_1: '',
  //   VARNAME_1: 'Madhya Pashchimanchal',
  //   TYPE_1: 'Vikas Kshetra',
  //   ENGTYPE_1: 'Development Region' },
  // { PID: '2059',




function stepThroughLocations(node, arr, parent) {

	var level, i, p;

	node = node || locationRef['adm1'][arr[0]];

	if (arr.length > 1 && node) {
		level = 'adm' + (parseInt(node.level.substr(3,1)) + 1);
		p = node.name;
		if (level !== 'adm5') {
			node = locationRef[level][arr[1]];
		}
		else {
			node = false;
		}
		
		//check if not found
		if (!node || Array.isArray(node)) {
			return false;
		}
		else {
			arr = arr.splice(1, arr.length-1);
			return stepThroughLocations(node, _.clone(arr), p);
		}
	}

	return node;

}

// { locations:
//    { merge: [ [Object], [Object], [Object], [Object] ],
//      a:
//       [ 'Central|Janakpur|Dhanusa|Lakhouri|(unk)|Other: Jhitkohiya Road',
//         'Central|Janakpur|Dhanusa|SugaMadhukarahi' ],
//      b:
//       [ 'Central|Narayani',
//         'Central|Narayani|Bara',
//         'Central|Narayani|Bara|Jhitakaiya(Dakshin)' ] } }

// [ { match: true,
//     source: 'MATCH',
//     value: 'Central',
//     cleanValue: '',
//     deleted: false },
//   { match: true,
//     source: 'MATCH',
//     value: 'Central|Janakpur',
//     cleanValue: '',
//     deleted: false },
//   { match: true,
//     source: 'MATCH',
//     value: 'Central|Janakpur|Dhanusa',
//     cleanValue: '',
//     deleted: false },
//   { match: true,
//     source: 'MATCH',
//     value: 'Central|Janakpur|Dhanusa|Lakhouri',
//     cleanValue: '',
//     deleted: false } ]



	// "ID" : "9",
	// 		"TENDER_NOTICE_ID" : "11",
	// 		"CODER_ID" : "",
	// 		"TENDERNOTICENUMBER" : "11/070-71",
	// 		"CONTRACTNUMBER" : "DROJKR/337107-4/070-71 /147",
	// 		"CONTRACTTYPE" : "CONSTRUCTION",
	// 		"PROJECTNAME" : "Road Construction",
	// 		"PROJECTNUMBER" : "",
	// 		"PROJECTFUNDER" : "",
	// 		"CONTRACTNAME" : "Mahadevpatti Halkhori Bathnaha Road, Mahottari District, Earth Work , Gravel etc.",
	// 		"CONTRACTDESCRIPTION" : " Earth Work , Gravel etc.",
	// 		"COSTESTIMATE" : "1689668",
	// 		"ESTIMATECURRENCY" : "NPR",
	// 		"DATASOURCE" : "",
	// 		"coder" : "amrita",
	// 		"hash" : "1111/070-71Closedh