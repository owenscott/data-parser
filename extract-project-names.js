//don't run this again until you have access to the work that dustin did

var async = require('async'),
	_ = require('underscore'),
	mongodb = require('mongodb');

var fs = require('fs');

var csv = '';

var MONGOHOST = 'localhost'
var DBs = ['contractqab1', 'contractqab2']
var MONGOPORT = '27017'

var HEADERS = ['status','tenderNoticeNumber', 'contractNumber', 'projectName', 'projectFunder', 'url', 'ampProjectID', 'actualProjectName', 'projectCode', 'fundingOrg', 'hash']

csv = csv + ('"' + HEADERS.join('","') + '"\n');

//iterate through all of the dbs
async.each(DBs, function(db, callback) {
	//open db
	mongodb.connect('mongodb://' + MONGOHOST + ':' + MONGOPORT + '/' + db, function(err, db) {
		//get contracts collection
		var collection = db.collection('contracts');
		collection.find().toArray(function(err, arr) {
			if (err) console.log(err);
			//iterate through each contract
			arr.forEach(function(record) {
				//get relevant fields
				var fields = [];
				fields.push(record.meta.status);
				fields.push(valueOrCleanValue(record.data.keyValuePairs.merge.TENDERNOTICENUMBER));
				fields.push(valueOrCleanValue(record.data.keyValuePairs.merge.CONTRACTNUMBER));
				fields.push(valueOrCleanValue(record.data.keyValuePairs.merge.PROJECTNAME));
				fields.push(valueOrCleanValue(record.data.keyValuePairs.merge.PROJECTFUNDER));
				fields.push(record.scraped.URL);
				//add blank values
				fields = fields.concat(['','','',''])
				fields.push(record.scraped.hash);
				csv = csv + ('"' + fields.join('","') + '"\n');
			})

			db.close();
			callback();
		});
		// console.log(db.collection('contracts').find().count());
	});
},
	function(err) {
		if (err) console.log(err)
		fs.writeFileSync('./output/projectNameMatching.csv', csv)
	}
)

function valueOrCleanValue(obj) {
	return obj.cleanValue || obj.value;
}