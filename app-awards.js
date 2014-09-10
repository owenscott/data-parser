//============================================
//||         INCLUDES AND CONF              ||
//============================================

//lib
var fs = require('fs'),
	async = require('async'),
	_ = require('underscore'),
	winston = require('winston'),
	path = require('path');

//src
var getFileInfo = require('./src/getFileInfo'),
	cleanNulls = require('./src/cleanNulls'),
	InsertParser = require('./sql-insert-to-json/app'),
	createHash = require('./src/createMD5'),
	createSupplierHash = require('./src/createSupplierHash');

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


//============================================
//||   MAIN FUNCTION FOR FILE PROCESSING    ||
//============================================

var processFile = function(file, callback) {
	
	var fileInfo = getFileInfo(file),
		rawData,
		scrapedAwards,
		codedAwards,
		codedSuppliers;

	
	//PROCESS FILE
	if (_.contains(conf.awardVersions, fileInfo.version)) {
		
		logger.info('Starting file', file, '\n<=========================================================>')
		rawData = parser.parse(fs.readFileSync(path.join(conf.dataDir, file)).toString().split('\n')); 
		
		scrapedAwards = rawData.AWARD;
		codedAwards = rawData.CODEDAWARD;
		codedSuppliers = rawData.CODEDSUPPLIER;


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
			tempAward.suppliers = _.where(codedSuppliers, {AWARD_ID: tempAward.codedAward.ID})

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
	logger.info('==========================================');

	logger.info('Done processing data files. ' + outputData.length + ' records found.');

	outputData.forEach(function(record) {

		var recordId = record.hash,
			cleanRecord;

		//clean up record a little bit
		if (record.codedAward || record.suppliers) {
			cleanRecord = _.clone(record.coded) || {};
			cleanRecord.scraped = record.scraped || {};
			cleanRecord.hash = recordId;
			cleanRecord.locations = _.clone(record.locations) || [];

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

}


//============================================
//||     CONTROL FLOW AND EXECUTION         ||
//============================================

fs.readdir(conf.dataDir, function(err, files) {
	async.each(files, processFile, combineDataByHash);
});