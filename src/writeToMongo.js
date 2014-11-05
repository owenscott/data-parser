var mongoClient = require('mongodb').MongoClient,
	mongoObjectId = require('mongodb').ObjectID,
	fs = require('fs'),
	async = require('async'),
	_ = require('underscore'),
	createHash = require('./createMD5');

var conf = JSON.parse(fs.readFileSync('./conf.json').toString());

var PREVIOUS_DBS = conf.awardCodedDBs,
	OUTPUT_DB = conf.awardCurrentDB;

var COLLECTION = 'contracts';

var completedContractHashes = [];

module.exports = function(data, logger) {


	logger.info('Writing to Mongo started with ' + data.length + ' contracts.');

	//go through previous dbs
	async.each(PREVIOUS_DBS, 
		function(dbName, callback) {
			mongoClient.connect('mongodb://' + conf.mongoHost + ':' + conf.mongoPort + '/' + dbName, function(err, db) {
				
				db.collection(COLLECTION).find().toArray(function(err, records) {
					
					records.forEach(function(record){

						if(record.meta.status === 'closed') {
							completedContractHashes.push(record.meta.id);
							if (!record.meta.id) {
								logger.error('warning: record with no hash')
							}
						}

					})

					db.close();
					callback();

				})
			}
		)
	},
		write
	)

	function write () {


		var excludedContracts = 0;
		logger.info('There are ' + completedContractHashes.length + ' previously completed contracts');

		var finalData = [];

		logger.log('There are', data.length, 'contracts in total')

		data.forEach(function(record) {
			if (!_.contains(completedContractHashes, record.meta.id)) {
				if (!record.meta.id) {
					console.log(record);
					console.log('warning: new record with no hash')
				}
				finalData.push(record);
			}
			else {
				excludedContracts++;
			}
		})

		logger.info('There are', excludedContracts, 'in this set which have been previously coded');
		logger.info('There are', finalData.length, 'going to the toolkit set');


		finalData.forEach(function(record) {
			record._id = mongoObjectId();
		});

		mongoClient.connect('mongodb://' + conf.mongoHost + ':' + conf.mongoPort + '/' + OUTPUT_DB, function(err, db) {
			db.collection(COLLECTION).drop();
			db.collection(COLLECTION).insert(finalData, function(err, data) {
				if(err) {throw err}
				db.close();
			});
		});

	}




}

