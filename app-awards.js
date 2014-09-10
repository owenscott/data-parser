//lib
var fs = require('fs'),
	async = require('async'),
	_ = require('underscore'),
	winston = require('winston'),
	path = require('path');

//src
var getFileInfo = require('./src/getFileInfo'),
	cleanNulls = require('./src/cleanNulls'),
	InsertParser = require('./sql-insert-to-json/app');

//conf
var conf = JSON.parse(fs.readFileSync('./conf.json').toString()),
	awardsSchema = JSON.parse(fs.readFileSync('./src/awardsSchema.json').toString());

//logger
var logger = new winston.Logger();
logger.add(winston.transports.Console, {colorize:true})

//parser
var parser = new InsertParser(awardsSchema);

//main function
var processFile = function(file, callback) {
	
	var fileInfo = getFileInfo(file),
		rawData,
		scrapedAwards,
		codedAwards,
		codedSuppliers;

	var outputData = [];
	
	//PROCESS FILE
	if (_.contains(conf.awardVersions, fileInfo.version)) {
		
		logger.info('Starting file', file, '\n======================================	')
		rawData = parser.parse(fs.readFileSync(path.join(conf.dataDir, file)).toString().split('\n')); 
		
		scrapedAwards = rawData.AWARD;
		codedAwards = rawData.CODEDAWARD;
		codedSuppliers = rawData.CODEDSUPPLIER;

		cleanNulls(scrapedAwards);
		cleanNulls(codedAwards);
		cleanNulls(codedSuppliers);

		scrapedAwards.forEach(function(award) {
			console.log(award);
			process.exit();
		})


	}

	//DON'T PROCESS FILE
	else {
		logger.info('File', file, 'ignored.')
		callback();
	}
	
}

//closing function
var combineDataByHash = function() {

}


//control flow
fs.readdir(conf.dataDir, function(err, files) {
	async.each(files, processFile, combineDataByHash);
});