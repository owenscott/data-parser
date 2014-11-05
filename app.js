//============================================
//||         INCLUDES AND CONF              ||
//============================================

//lib
var fs = require('fs'),
	async = require('async'),
	_ = require('underscore'),
	winston = require('winston'),
	path = require('path'),
	jsonMerge = require('json-premerge'),
	mongoClient = require('mongodb'),
	csvParse = require('csv-parse');

//src
var getFileInfo = require('./src/getFileInfo'),
	cleanNulls = require('./src/cleanNulls'),
	InsertParser = require('./sql-insert-to-json/app'),
	createHash = require('./src/createMD5'),
	createSupplierHash = require('./src/createSupplierHash'),
	writeToMongo = require('./src/writeToMongo');

//conf
var conf = JSON.parse(fs.readFileSync('./conf.json').toString()),
	awardsSchema = JSON.parse(fs.readFileSync('./src/awardsSchema.json').toString());

//logger
var logger = new winston.Logger();
logger.add(winston.transports.Console, {colorize:true})

//parser
var parser = new InsertParser(awardsSchema);

//global
var outputData = [];


function getTableName(statement) {
	var insertSubStr = 'INSERT INTO '
	return statement.substr(insertSubStr.length, statement.indexOf(' VALUES') - insertSubStr.length ).replace(/"/g, '');
}

//============================================
//||   MAIN FUNCTION FOR FILE PROCESSING    ||
//============================================

var processFile = function(file, callback) {
	
	var fileInfo = getFileInfo(file),
		rawData,
		parsedData = {},
		scrapedAwards,
		codedAwards,
		codedSuppliers;

	
	//PROCESS FILE
	if (_.contains(conf.awardVersions, fileInfo.version)) {
		
		//convert raw data :(
		logger.info('Starting file', file, '\n<=========================================================>')
		// rawData = parser.parse(fs.readFileSync(path.join(conf.dataDir, file)).toString().split('\n'));
		rawData = fs.readFileSync(path.join(conf.dataDir, file), 'utf8').toString();
		rawData = rawData.replace(/"CONSTRUCTION OF RAPTI KHOLA BRIDGE"/g, 'CONSTRUCTION OF RAPTI KHOLA BRIDGE')

		rawData = rawData.split('\n');

		async.each(rawData,
		function(row, callback)  {
			var insertLength = row.match(/INSERT INTO (")?[a-zA-Z0-9]*(")? VALUES/)[0].length;
			var tableName = getTableName(row);
			var valueString = row.substr(insertLength + 1, row.length - insertLength - 2);
			csvParse(valueString, {quote:'\''}, function(err, data) {
				if(err) {
					console.log(file);
					console.log(valueString);
					throw new Error(err);					
				}
				var tempObj = {};
				parsedData[tableName] = parsedData[tableName] || [];

				awardsSchema[tableName].forEach(function(key, i) {
					tempObj[key] = data[0][i]
				})

				//hacky sanity check
				var expectedLengths = {
					'CODEDAWARD': 8,
					'CODEDSUPPLIER': 14,
					'AWARD': 16
				}

				if (data[0].length !== expectedLengths[tableName]) {
					logger.error('A record has the wrong length');
					throw new Error();
				}

				parsedData[tableName].push(tempObj);
				callback();
			})
		},
		function(){
			


			scrapedAwards = parsedData.AWARD;
			codedAwards = parsedData.CODEDAWARD;
			codedSuppliers = parsedData.CODEDSUPPLIER;



			cleanNulls(scrapedAwards);
			cleanNulls(codedAwards);
			cleanNulls(codedSuppliers);

			scrapedAwards.forEach(function(award) {

				var tempAward = {};

				//create hash
				tempAward.hash = createHash(award);

				//add scraped data to record
				tempAward.scraped = award;

				//add coded award data to record
				tempAward.codedAward = _.findWhere(codedAwards, {AWARDID: tempAward.scraped.ID});

				//add coded supplier data to record and sanity check on coded dat
				tempAward.suppliers = _.where(codedSuppliers, {AWARD_ID: tempAward.scraped.ID})

				//sanity checks
				if (!tempAward.codedAward) {
					logger.warn('No coded award data for', fileInfo.coder, 'record', tempAward.scraped.ID)
				}
				if (!tempAward.suppliers || _.isEmpty(tempAward.suppliers)) {
					logger.warn('No coded supplier data for', fileInfo.coder, 'record', tempAward.scraped.ID)
				}

				//create hashes for supplier matching
				tempAward.suppliers.forEach(function(supplier) {
					supplier.hash = createSupplierHash(supplier)
				})

				outputData.push(_.clone(tempAward));

			})

			logger.info('Done file', '\n</=======================================================>')
			callback();

		})

	}

	//DON'T PROCESS FILE
	else {
		logger.info('File', file, 'ignored.')
		callback();
	}
	
}


//============================================
//||  FUNCTION FOR COMBINGING DATA BY HASH  ||
//============================================


var combineDataByHash = function() {


	var combinedData = {},
		goodRecords = [],
		shortRecords = [],
		longRecords = [],
		badRecords = [],
		results = [],
		mergedResults = [];

	logger.info('==========================================');
	logger.info('==========================================');

	logger.info('Done processing data files. ' + outputData.length + ' records found.');

	outputData.forEach(function(record) {

		var recordId = record.hash,
			cleanRecord;

		//clean up record a little bit
		if (record.codedAward || record.suppliers) {
			cleanRecord = _.clone(record.codedAward) || {};
			cleanRecord.scraped = record.scraped || {};
			cleanRecord.hash = recordId;
			cleanRecord.suppliers = _.clone(record.suppliers) || [];

			//push into results array
			combinedData[recordId] = combinedData[recordId] || [];
			combinedData[recordId].push(_.clone(cleanRecord));
		}

	})


	//look for records with the right length
	_.keys(combinedData).forEach(function(hash) {
		if (combinedData[hash].length === 2) {
			goodRecords.push(_.clone(combinedData[hash]));
		}
		else if (combinedData[hash].length === 1) {
			shortRecords.push(_.clone(combinedData[hash]));
		}
		else if (combinedData[hash].length > 2) {
			longRecords.push(_.clone(combinedData[hash]));
		}
		else {
			badRecords.push(_.clone(combinedData[hash]))
		}
	});


	if (shortRecords.length) {logger.warn(shortRecords.length + ' hashes found with only one record.')}
	if (longRecords.length) {logger.warn(longRecords.length + ' hashes found with too many records.')};
	if( badRecords.length) {logger.warn(badRecords.length + ' hashes found that seem really wrong.');}
	if (goodRecords.length) {logger.info(goodRecords.length + ' hashes found that look good')} else {logger.warn('No hashes found that look good');}

	logger.info('==========================================');
	logger.info('Putting data into correct merge format');

	//add good records
	results = results.concat(goodRecords);

	//add long records
	longRecords.forEach(function(record) {
		var shortenedRecord = [record[0], record[1]];
		results.push(shortenedRecord);
	});

	//add short records if enabled
	if (conf.includeShortRecords === true) {
		logger.warn('User has enabled the inclusion of short records (with only one coder)')
		shortRecords.forEach(function(record) {
			var lengthenedRecord = [];
			lengthenedRecord[0] = record[0];
			lengthenedRecord[1] = {};
			_.keys(record[0]).forEach(function(key){
				if(Array.isArray(record[0][key])) {
					lengthenedRecord[1][key] = [];
				}
				else {
					lengthenedRecord[1][key] = '';
				}
			})
			results.push(lengthenedRecord);
		})
	}

	logger.info(results.length + ' records are ready to be merged');

	//MERGE FINAL RECORDS
	async.each(results,
	function(result, callback) {

		//save scraped data as separate object
		var scraped = _.clone(result[0].scraped);

		var recordId = result[0].hash;

		// remove scraped data from each array member
		result = _.map(result, function(r) {
			return _.clone(_.omit(r, 'scraped'));
		})


		//merge JSON
		jsonMerge(result, function(err, data) {
			var temp = {};

			temp.data = _.clone(data);
			
			temp.meta = {
				status: 'open',
				workLog: [],
				id: recordId
			};
			
			temp.scraped = scraped;

			mergedResults.push(temp);

			callback();

		});
		


	},

	//WRITE FINAL RECORDS TO MONGO
	function(err) {
		console.log('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')

		logger.info(mergedResults.length + ' records will be written into the final dataset.');

		logger.info('----------------------------------------------------------------------')
		logger.info('But first...we must shadily convert the merged supplier data to locations');
		mergedResults.forEach(function(record) {
			var suppliers = record.data.arrays.originals.suppliers;
			var locations = [[],[]];
			suppliers.forEach(function(setOfSuppliers, i) {
				setOfSuppliers.forEach(function(supplier) {
					locations[i].push(makeSupplierIntoString(supplier));							

				})
			})
			locations[0] = _.uniq(locations[0]);
			locations[1] = _.uniq(locations[1]);
			var mergedLocations = _.intersection(locations[0], locations[1]);
			console.log(locations);
			console.log(mergedLocations);
			console.log('===================')

			//put it into the record
			record.data.arrays.merge['locations'] = mergedLocations;
			record.data.arrays.originals['locations']= locations;


			// process.exit();

		})



		//TODO: need to make sure aren't previously coded
		writeToMongo(mergedResults, logger);
	});

}


function makeSupplierIntoString(supplier) {
	var supplierString = '';
	var LOCATION_KEYS = ['ADM1', 'ADM2', 'ADM3', 'ADM4']
	var locationString = '';

	supplierString += 'Supplier: ' + (supplier.SUPPLIER_NAME || 'Unknown');

	if (supplier.SUPPLIER_REGISTRATION_CODE) {
		supplierString += ', RegistrationCode:' + supplier.SUPPLIER_REGISTRATION_CODE;
	}

	for (var i = 0; i < LOCATION_KEYS.length; i++) {
		
		if (supplier[LOCATION_KEYS[i]]) {
			locationString = locationString + supplier[LOCATION_KEYS[i]] + '/'
		}
	}

	if (locationString) {
		supplierString += ', Location: ' + locationString;
	}


	return supplierString;
}


//============================================
//||     CONTROL FLOW AND EXECUTION         ||
//============================================

fs.readdir(conf.dataDir, function(err, files) {
	async.eachSeries(files, processFile, combineDataByHash);
});