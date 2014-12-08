var fs = require('fs'),
	_ = require('underscore'),
	csvParse = require('csv-parse'),
	async = require('async'),
	mongoClient = require('mongodb');


var	data = [];

var conf = JSON.parse(fs.readFileSync('./conf.json').toString());

var supplierOmits = ['ID', 'AWARD_ID', 'CODER_ID', 'WARD', 'OTHER_LOCATION', 'OTHER_LOCATION_DESC', 'hash']

var fromScraped = ['URL', 'DATE1', 'DATE2']

var mapKey = function(key) {

	var mapping = {
		TENDERNOTICENUMBER: 'tenderNoticeNumber',
		CONTRACTNUMBER: 'contractNumber',
		STATUS: 'status',
		URL: 'documentURL',
		COSTESTIMATE: 'costEstimate',
		COSTESTIMATECURRENCY: 'costEstateCurrency',
		AWARDAMOUNT: 'awardAmount',
		AWARDAMOUNTCURRENCY: 'awardAmountCurrency',
		DATE1: 'contractDate',
		DATE2: 'completionDate',
		SUPPLIER_NAME: 'supplierName',
		SUPPLIER_REGISTRATION_CODE: 'supplierRegistrationCode',
		SUPPLIER_REGISTRATION_CODE_SOURCE: 'supplierRegistrationCodeSource',
		SUPPLIER_COUNTRY: 'supplierCountry'
	}

	return mapping[key] || undefined;

}


//TODO: change this to QAd values instead of coded values

var outputDbs = conf.awardCodedDBs;

console.log('Loading data for: ', JSON.stringify(outputDbs));

async.each(
	
	outputDbs,

	//FOR EACH DB
	function(dbName, callback) {

		console.log('Running', dbName);

		mongoClient.connect('mongodb://' + conf.mongoHost + ':' + conf.mongoPort + '/' + dbName, function(err, db) {

			db.collection('contracts').find().toArray(function(err, records) {

				console.log(records.length, 'records for', dbName);

				records.forEach(function(record) {
					

					var tempRecord = {};

					//add scraped fields
					fromScraped.forEach(function(key) {
						tempRecord[mapKey(key)] = record.scraped[key]
						if (mapKey(key) === 'completionDate') {
							if (!record.scraped[key].match(/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/)) {
								// tempRecord.badDate = true;
							}
							else {
								// tempRecord.badDate = false;
							}
						}
					})

					var mergedData = record.data.keyValuePairs.merge;

					//add coded fields
					_.keys(mergedData).forEach(function(key) {
						// if (mergedData[key].match === true && mapKey(key)) {
						// 	tempRecord[mapKey(key)] = mergedData[key].value;
						// }
						if ( mapKey(key) && ( mergedData[key].cleanValue || mergedData[key].value ) ) {
							tempRecord[mapKey(key)] = mergedData[key].cleanValue || mergedData[key].value;
						} 
					})
					
					//add suppliers
					//TODO: switch this approach post-coding
					//TODO: actually, go back to merge and merge based on the key

					var nameHashs = _.chain(suppliers).map(function(s) {return s.hash.complete}).value();
					var idHashs = _.map(suppliers, function(s) {return s.hash.code});

					// var suppliers = record.data.arrays.originals.suppliers[0].concat(record.data.arrays.originals.suppliers[1]);
					var supplierStrings = record.data.arrays.merge.locations;
					
	

					var suppliers = supplierStrings.map(parseSupplierString);



					function duplicateHashs(hashes) {
						var history = {};
						var output = [];
						hashes.forEach(function(hash) {
							if (history[hash]) {
								output.push(hash);
							}
							else {
								history[hash] = 1;
							}
						})
						output = _.uniq(output);
						return output
					}

					
					//reduce the hash arrays to only have duplicates
					idHashs = duplicateHashs(idHashs);
					nameHashs = duplicateHashs(nameHashs);

					function simplifySupplier (supplier) {
						return _.omit(supplier, supplierOmits)
					}


					var finalSuppliers = [];

					suppliers.forEach(function(supplier) {
						// if (_.contains(nameHashs, supplier.hash.complete)) {
						// 	finalSuppliers.push(simplifySupplier(supplier));
						// }
						// else if(supplier.hash.code && _.contains(idHashs, supplier.hash.code)) {
						// 	finalSuppliers.push(simplifySupplier(supplier));
						// }

						finalSuppliers.push(simplifySupplier(supplier));
					})



					finalSuppliers = _.uniq(finalSuppliers, function(m) {return JSON.stringify(m)});

					tempRecord.suppliers = finalSuppliers;
					data.push(tempRecord);

				})

				// console.log(data);
				console.log('data length is now', data.length);
				db.close();
				callback();


			})



		})

	},
	
	//ONCE ALL DBs are DONE
	function(err) {

		fs.writeFileSync('./output/nepal-awards-' + new Date().toISOString() + '-release.json', JSON.stringify(data));

	}

);



// // ----------------------------------------------
// // |       PROCESS RAW DATA TO BETTER JSON      |
// // ----------------------------------------------


// rawData.forEach(function(record) {
	
// 	var tempRecord = {},
// 		tempKvps = {},
// 		locations;

// 		console.log(record);
// 		process.exit();

// 	//translate data to new format

// 	_.keys(record.data.keyValuePairs.merge).forEach(function(key) {
// 		if (record.data.keyValuePairs.merge[key].match && mapKey(key)) {
// 			tempKvps[mapKey(key)] = record.data.keyValuePairs.merge[key].value;
// 		}
// 	})

// 	//clean data

// 	if (tempKvps['contractIssuer']) {
// 		tempKvps['contractIssuer'] = tempKvps['contractIssuer'].replace('Issued by\\u0009','')
// 	}

// 	if (tempKvps['contractType'] && tempKvps['contractType'] === 'MULTIPLE TYPES') {
// 		delete tempKvps['contractType'];
// 	}

// 	locations = _.pluck(record.data.arrays.merge.locations,'value');

// 	// console.log(locations);

// 	//more cleaning data
	
// 	if (!_.isEmpty(tempKvps)) {
// 		_.extend(tempRecord, tempKvps);
// 	}

// 	if (locations.length > 0) {
// 		tempRecord.locations = _.clone(locations);
// 	}

// 	data.push(_.clone(tempRecord));

// })

// // ----------------------------------------------
// // |            PROCESS RAW GADM DATA           |
// // ----------------------------------------------

// var gadmRaw = [],
// 	gadm = [],
// 	i = 0;

// gadmRaw[0] = fs.readFileSync('./ref/gadm-adm1.csv').toString();
// gadmRaw[1] = fs.readFileSync('./ref/gadm-adm2.csv').toString();
// gadmRaw[2] = fs.readFileSync('./ref/gadm-adm3.csv').toString();
// gadmRaw[3] = fs.readFileSync('./ref/gadm-adm4.csv').toString();

// async.eachSeries(gadmRaw, function(csv, callback) {
// 	csvParse(csv, {columns:true}, function(err, data) {
// 		gadm[i] = data;
// 		i++;
// 		callback();
// 	})
// },
// 	function( ){

// 		var csvOutput = [],
// 		csvText = '';

// 		//clean up gadm data
// 		for (i in gadm) {
// 			gadm[i] = _.map(gadm[i], function(record) {
// 				var level = parseInt(i) + 1;
// 				return {
// 					level: 'adm' + level,
// 					name: record['NAME_' + level],
// 					pid: record.PID,
// 					type: record['ENGTYPE_' + level],
// 					levelId: record['ID_' + level]
// 				}
// 			});
// 		}

// 		// //add gadm data to locations and convert to flat csv output
// 		data.forEach(function(d) {


// 			//add gadm data
// 			d.locations = _.map(d.locations, function(location) {
// 				var locArr = [];
// 				var splitLocations = location.split('|');

// 				for (var i = 0; i < splitLocations.length; i++) {
// 					if(!_.isEmpty(_.findWhere(gadm[i], {name:splitLocations[i]}))) {
// 						locArr.push(_.findWhere(gadm[i], {name:splitLocations[i]}))
// 					}

// 				}

// 				return _.flatten(locArr);
				
// 			})

// 			d.locations = _.flatten(d.locations);
// 			d.locations = _.uniq(d.locations, function(d) {return JSON.stringify(d)});

// 			if (d.locations.length <= 0) {
// 				delete d.locations;
// 			}

// 			if (_.isEmpty(d)) {
// 				delete d;
// 			}
	
// 		})


// 		// csvText = csvText + _.keys(csvOutput[0]).join(',') + '\r\n';
		
// 		// csvOutput.forEach(function(result) {

// 		// 	var arr;
			
// 		// 	arr = _.map( _.values(result), function(val) {
// 		// 		val = val.replace(/,/,'');
// 		// 		val = val.replace(/"/,'');
// 		// 		return val;
// 		// 	})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
// 		// 	csvText = csvText + arr.join(',');
// 		// 	csvText = csvText + '\r\n';
// 		// });

// 		// fs.writeFileSync('./output/output-with-locations.json', JSON.stringify(results));
// 		// fs.writeFileSync('./output/output-with-locations.csv', csvText);
// 		fs.writeFileSync('./output/draft-output-for-anjesh-7-30.json', JSON.stringify(data));
// 	}
// );


function parseSupplierString(supplierString) {
	supplierString = supplierString.value;	
	var locationOfSupplier = supplierString.search(/Supplier: /);
	var locationOfCode = supplierString.search(/RegistrationCode:/);
	var locationOfLocation = supplierString.search(/Location: /);

	var location = supplier = code = '';

	// crappy code to find supplier
	if (locationOfCode >= 0 ) {
		var supplier = supplierString.substr(10, locationOfCode - 12);
	}
	else if (locationOfLocation >= 0) {
		var supplier = supplierString.substr(10, locationOfLocation - 12);
	}
	else if (locationOfSupplier>= 0) {
		var supplier = supplierString.substr(10, supplierString.length);
	}

	//crappy code to find registration code
	if(locationOfCode >= 0) {
		if (locationOfLocation >= 0) {
			var code = supplierString.substr(locationOfCode + 'RegistrationCode:'.length, locationOfLocation - locationOfCode - 'RegistrationCode:'.length - 2).trim();
		}
		else {
			var code = supplierString.substr(locationOfCode + 'RegistrationCode:'.length, supplierString.length).trim();
		}
	}

	if(locationOfLocation >= 0) {
		var location = supplierString.substr(locationOfLocation + 'Location: '.length, supplierString.length).trim();
		var locationArray = location.split(/\//);
	}

	var resultObj = {}

	if (supplier) {
		resultObj['supplierName'] = supplier;
	}
	if (code) {
		resultObj['supplierRegistrationCode'] = code;
	}
	if (locationArray) {
		locationArray.forEach(function(loc, i) {
			if (loc.trim()) {
				resultObj['adm' + (i+1)] = loc									
			} 

		})
	}
	//crappy code to find location
	return resultObj;

}