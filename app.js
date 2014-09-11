//necessary merge

var fs = require('fs'),
	_ = require('underscore'),
	InsertParser = require('./sql-insert-to-json/app.js'),
	winston = require('winston'),
	jsonMerge = require('json-premerge'),
	logger,
	VERSION = '0.2',
	logger,
	schema,
	jsonData = [],
	async = require('async'),
	processFile,
	combineDataByHash,
	DATA_DIR = './data',
	cleanNulls,
	writeFile,
	mapLocations,
	mapLocation,
	locationRef,
	crypto = require('crypto');

locationRef = JSON.parse(fs.readFileSync('./ref/locations.json').toString());
logger = new winston.Logger();

logger.add(winston.transports.Console, {colorize:true})

// var codedHashes = JSON.parse(fs.readFileSync('./completed-batches/codedHashes-b1.json').toString());

var conf = JSON.parse(fs.readFileSync('./conf.json').toString());


cleanNulls = function(arr) {
	arr.forEach(function(obj) {
		_.each(_.keys(obj), function(key) {
			if (obj[key] === 'NULL' || obj[key] === 'NULL)') {
				obj[key] = '';
			}
		})
	})
}


mapLocation = function(location) {

	var levels,
	lev,
	l, 
	i,
	result = [];

	levels = ['adm4', 'adm3', 'adm2', 'adm1'];

	if (location.OTHER_LOCATION) {
		result.unshift('Other: ' + location.OTHER_LOCATION) 
	}
	else {
		result.unshift('');
	}

	for (l in levels) {

		lev = levels[l];

		if (location[lev.toUpperCase()]) {

			for (i = l; i >= 0; i-- ) {
				result.unshift('(unk)');
			}

			addLocations(result, location, location[lev.toUpperCase()], lev);
			break;
		}

	}

	return result;

}

function addLocations (arr, location, name, lev) {


	var levels = ['adm4', 'adm3', 'adm2', 'adm1'],
	ref,
	nextLevel;

	ref = locationRef[lev][name];

	if(!ref) {
		logger.warn('Can\'t find level ' + lev + ' location ' + name);
		arr = [];
		return;
	}

	nextLevel = levels[levels.indexOf(lev) + 1];

	if (Array.isArray(ref) ) {
		//if it's an array use the coded parent or else just give up
		if (location[nextLevel.toUpperCase()]) {
			arr.unshift(name);
			addLocations( arr, location, location[nextLevel.toUpperCase()] , nextLevel );
		}
		else {
			logger.warn('Level ' + lev + ' location ' + name + ' has multiple matches and no coded parent.');
			arr = [];
			return;
		}

	}
	else {

		arr.unshift(ref.name);
		
		if (nextLevel) {
			addLocations( arr, location, locationRef[nextLevel][ref.parent].name, nextLevel );
		}
	}

}

mapLocations = function(locations) {
	var result = [];

	locations.forEach (function(location) {

		var mappedLocation,
		i;

		mappedLocation = mapLocation(location);

		for (i = 0; i < mappedLocation.length; i++ ) {

			if(mappedLocation[i] && mappedLocation[i] !== '(unk)') {
				result.push(mappedLocation.slice(0,i+1).join('|'));
			}

		}


		


	})
	

	result = _.uniq(result);


	return result;
}


//callback for file processing
processFile = function(file, callback) {

	if (file.substr(0,2) !== 'a1') {
		var coderName = file.substr(3,file.indexOf('.sql') - 3);

		fs.readFile( DATA_DIR + '/' + file, function (err, data) {


			//TODO: version handling not through a (non)constant and make it more strucutred in the file name

			if (file.substr(0,2) === 'b1' || file.substr(0,2) === 'b2' ) {
				VERSION = '0.1';
			}
			else if (file.substr(0,2) === 'b3' || file.substr(0,2) === 'b4') {
				VERSION = '0.2';
			}

			else {
				VERSION = '0.3';
			}

			



			//set schema
			if( VERSION === '0.1') {
				schema = {
					TENDERNOTICE: ['ID','TENDERNOTICENUMBER','STATUS','URL','CONTRACTDETAILS','ISSUER','PUBLICATIONDATE','PUBLISHEDIN','DOCUMENTPURCHASEDEADLINE','SUBMISSIONDEADLINE','OPENINGDATE','CONTRACTNAME','CONTRACTDESCRIPTION','COSTESTIMATE','ESTIMATECURRENCY','DATASOURCE'],
					CODEDTENDERNOTICE: ['ID','TENDER_NOTICE_ID','CODER_ID','TENDERNOTICENUMBER','CONTRACTNUMBER','CONTRACTTYPE','PROJECTNAME','PROJECTNUMBER','PROJECTFUNDER','CONTRACTNAME','CONTRACTDESCRIPTION','COSTESTIMATE','ESTIMATECURRENCY','DATASOURCE'],
					CODEDLOCATION: ['ID','CODED_TENDER_NOTICE_ID','ADM1','ADM2','ADM3','ADM4','WARD','OTHER_LOCATION','OTHER_LOCATION_DESC','ACTIVITY_DESC']
				}
			}
			else if ( VERSION === '0.2') {
				schema = {
					TENDERNOTICE: ['ID','TENDERNOTICENUMBER','STATUS','URL','CONTRACTDETAILS','ISSUER','PUBLICATIONDATE','PUBLISHEDIN','DOCUMENTPURCHASEDEADLINE','SUBMISSIONDEADLINE','OPENINGDATE','CONTRACTNAME','CONTRACTDESCRIPTION','COSTESTIMATE','ESTIMATECURRENCY','DATASOURCE','AGENCY','HASH','CODER1','CODER2'],
					CODEDTENDERNOTICE: ['ID','TENDER_NOTICE_ID','CODER_ID','TENDERNOTICENUMBER','CONTRACTNUMBER','CONTRACTTYPE','PROJECTNAME','PROJECTNUMBER','PROJECTFUNDER','CONTRACTNAME','CONTRACTDESCRIPTION','COSTESTIMATE','ESTIMATECURRENCY','DATASOURCE','NOTES'],
					CODEDLOCATION:  ['ID','CODED_TENDER_NOTICE_ID','ADM1','ADM2','ADM3','ADM4','WARD','OTHER_LOCATION','OTHER_LOCATION_DESC','ACTIVITY_DESC']
				}
			}
			else {
				schema = {
					TENDERNOTICE: ['ID','TENDERNOTICENUMBER','STATUS','URL','CONTRACTDETAILS','ISSUER','PUBLICATIONDATE','PUBLISHEDIN','DOCUMENTPURCHASEDEADLINE','SUBMISSIONDEADLINE','OPENINGDATE','CONTRACTNAME','CONTRACTDESCRIPTION','COSTESTIMATE','ESTIMATECURRENCY','DATASOURCE','AGENCY','HASH','CODER1','CODER2'],
					CODEDTENDERNOTICE: ['ID', 'TENDER_NOTICE_ID', 'CODER_ID', 'TENDERNOTICENUMBER', 'CONTRACTNUMBER', 'GOODS', 'SERVICES', 'CONSTRUCTION', 'MAINTENANCE', 'PROJECTNAME', 'PROJECTNUMBER', 'PROJECTFUNDER', 'CONTRACTNAME', 'CONTRACTDESCRIPTION', 'COSTESTIMATE', 'ESTIMATECURRENCY', 'DATASOURCE', 'NOTES'],
					CODEDLOCATION:  ['ID','CODED_TENDER_NOTICE_ID','ADM1','ADM2','ADM3','ADM4','WARD','OTHER_LOCATION','OTHER_LOCATION_DESC','ACTIVITY_DESC']
				}
			}
			
			if(err) {throw err};

			var statements = data.toString().split('\n'),
				parser = new InsertParser(schema),
				rawData,
				tenderNotices,
				codedTenderNotices,
				codedLocations,
				results = [],
				resultObj,
				assignCoderName,
				coder = coderName;

			logger.info('starting ' + file);
			rawData = parser.parse(statements);


			//raw data contains three tables which need to be split up
			tenderNotices = rawData.TENDERNOTICE;
			codedTenderNotices = rawData.CODEDTENDERNOTICE;
			codedLocations = rawData.CODEDLOCATION;

			cleanNulls(tenderNotices);
			cleanNulls(codedTenderNotices);
			cleanNulls(codedLocations);

			//only needs to be done because I stupidly left this out of the first version of the schema
			assignCoderName = function (record) {
				// console.log(file.substr(3, file.length - 7));
				record['coder'] = ''; //TODO: implement this based on file name
			}

			codedTenderNotices.map (assignCoderName);
			codedLocations.map(assignCoderName);

			tenderNotices.forEach( function (tenderNotice) {

				var hash = '';

				//resultObj is the result of the merging of all three data sets
				resultObj = {};
				resultObj.scraped = _.clone(tenderNotice);

				//create hash
				if (!resultObj.scraped.hash) {
					_.each(resultObj.scraped, function(i) {
						hash = hash + i;
					})
					resultObj.scraped.hash = hash;
				}

				resultObj.coded = _.findWhere(codedTenderNotices, {TENDER_NOTICE_ID: resultObj.scraped.ID});
				if (resultObj.coded) {
					resultObj.locations = _.where(codedLocations, {CODED_TENDER_NOTICE_ID: resultObj.coded.ID});
				}
				else {
					logger.warn('No coded data for ' + coderName + ' record ' + resultObj.scraped.ID);
				}
				
				//sanity check
				if (_.where(codedTenderNotices, {TENDER_NOTICE_ID: resultObj.scraped.ID}).length > 1 ) {
					logger.warn ('Multiple coded matches for ' + coderName + ' record ' + resultObj.scraped.ID);
				}

				//process locations
				if (resultObj.locations) {
					resultObj.locations = mapLocations(resultObj.locations);
				}

				//add resultObject to output if it hasn't been coded already in another batch
				// if (!_.contains(codedHashes, resultObj.scraped.hash)) {
				results.push(resultObj);				
				// }



			})

			jsonData = jsonData.concat(results);

			callback();

		});
	}
	else {
		//awards
		callback();
	}


}

writeFile = function(data) {

}

combineDataByHash = function() {

	var combinedData = {},
		goodRecords = [],
		shortRecords = [],
		longRecords = [],
		badRecords = [],
		results = [],
		mergedResults = [];

	logger.info('==========================================');
	logger.info('Done processing data files. ' + jsonData.length + ' records found.');

	jsonData.forEach(function(record) {

	var recordId = record.scraped.hash,
			cleanRecord;

			//clean up record a little bit
		if (record.coded || record.locations) {
			cleanRecord = _.clone(record.coded) || {};
			cleanRecord.scraped = record.scraped || {};
			cleanRecord.hash = recordId;
			cleanRecord.locations = _.clone(record.locations) || [];

			//push into results array
			combinedData[recordId] = combinedData[recordId] || [];
			combinedData[recordId].push(_.clone(cleanRecord));
		}

	})
	var dones = [];
	
	//look for records with the right length
	_.keys(combinedData).forEach(function(hash) {
		if (combinedData[hash].length === 2) {
			goodRecords.push(_.clone(combinedData[hash]));
			dones.push(hash);
		}
		else if (combinedData[hash].length === 1) {
			shortRecords.push(_.clone(combinedData[hash]));
		}
		else if (combinedData[hash].length > 2) {
			longRecords.push(_.clone(combinedData[hash]));
			dones.push(hash);
		}
		else {
			badRecords.push(_.clone(combinedData[hash]))
		}
	});

	fs.writeFileSync('./output/dones.json', JSON.stringify(dones));
	
	if (shortRecords.length) {logger.warn(shortRecords.length + ' hashes found with only one record.')}
	if (longRecords.length) {logger.warn(longRecords.length + ' hashes found with too many records.')};
	if( badRecords.length) {logger.warn(badRecords.length + ' hashes found that seem really wrong.');}
	if (goodRecords.length) {logger.info(goodRecords.length + ' hashes found that look good')} else {logger.warn('No hashes found that look good');}

	logger.info('==========================================');
	logger.info('Putting data into correct merge format');

	results = results.concat(goodRecords);

	longRecords.forEach(function(record) {
		var shortenedRecord = [record[0], record[1]];
		results.push(shortenedRecord);
	});

	// if (conf.includeShortRecords === true) {
	// 	shortRecords.forEach(function(record) {
	// 		var lengthenedRecord = [];
	// 		lengthenedRecord[0] = record[0];
	// 		lengthenedRecord[1] = {};
	// 		_.keys(record[0]).forEach(function(key){
	// 			if(Array.isArray(record[0][key])) {
	// 				lengthenedRecord[1][key] = [];
	// 			}
	// 			else {
	// 				lengthenedRecord[1][key] = '';
	// 			}
	// 		})
	// 		results.push(lengthenedRecord);
	// 	})
	// }



	logger.info('==========================================');
	logger.info('Merging ' + results.length + ' records');

	//merge all of the data into the a-b format and output to file
	async.each(results, function(result, callback) {
		
		
		var scraped = _.clone(result[0].scraped);
		
		result = _.map(result, function(r) {
			return _.clone(_.omit(r, 'scraped'));
		})
		
		
		jsonMerge(result, function(err, data) {
			var temp = {};
			//TODO: add hash here as ID
			
			temp.data = _.clone(data);
			
			temp.meta = {
				status: 'open',
				workLog: []
			};
			
			temp.scraped = scraped;
			
			mergedResults.push(temp);
			callback()
		});
	},
	function() {
		logger.info(mergedResults.length + ' merged records created');
		fs.writeFileSync('./output/output.json', JSON.stringify(mergedResults));
		logger.info('Data written to file.');
	});

}



//read every file and write the output
logger.info('Processing data files...');
fs.readdir(DATA_DIR, function(err, files) {
	async.each(files, processFile, combineDataByHash);
});
