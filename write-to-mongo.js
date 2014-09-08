var mongoClient = require('mongodb').MongoClient,
	mongoObjectId = require('mongodb').ObjectID,
	fs = require('fs'),
	async = require('async'),
	_ = require('underscore');

var PREVIOUS_DBS = ['contractqab1', 'contractqab2'];
var OUTPUT_DB = 'contractqab3';

var completedContractHashes = [];

//go through previous dbs
async.each(PREVIOUS_DBS, 
	function(dbName, callback) {
		mongoClient.connect('mongodb://localhost:27017/' + dbName, function(err, db) {
			db.collection('contracts').find().toArray(function(err, records) {
				
				records.forEach(function(record){

					//re-write hash (earlier records used a different approach)

					var temphash = makehash(record.scraped);
					// console.log(record.scraped.URL);
					// console.log(record.scraped.hash === temphash);
					// console.log(record.scraped.hash);

					if(record.meta.status === 'closed') {
						completedContractHashes.push(record.scraped.hash);
						if (!record.scraped.hash) {
							console.log('warning: record with no hash')
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


function makehash(obj) {
	
	var temp = '';

	_.each(_.omit(obj, 'hash'), function(val) {
		temp += val;
	})

	return temp;
}

function write () {


	var excludedContracts = 0;
	console.log('There are ' + completedContractHashes.length + ' previously completed contracts');

	fs.readFile('./output/output.json', function(err, data) {

		var jsonData = JSON.parse(data.toString());	
		var finalData = [];

		console.log('There are', jsonData.length, 'contracts in total')

		jsonData.forEach(function(record) {
			var temphash = makehash(record.scraped);
			if (!_.contains(completedContractHashes, record.scraped.hash)) {
				if (!record.scraped.hash) {
					console.log('warning: new record with no hash')
				}
				finalData.push(record);
			}
			else {
				excludedContracts++;
			}
		})

		console.log('There are', excludedContracts, 'in this set which have been previously coded');
		console.log('There are', finalData.length, 'going to the toolkit set');


		finalData.forEach(function(record) {
			record._id = mongoObjectId();
		});

		mongoClient.connect('mongodb://localhost:27017/' + OUTPUT_DB, function(err, db) {
			db.collection('contracts').drop();
			db.collection('contracts').insert(finalData, function(err, data) {
				if(err) {throw err}
				db.close();
			});
		});

	});

}