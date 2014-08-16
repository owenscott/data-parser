var mongoClient = require('mongodb').MongoClient,
	mongoObjectId = require('mongodb').ObjectID,
	fs = require('fs');

fs.readFile('./output/output.json', function(err, data) {

	var jsonData = JSON.parse(data.toString());	

	jsonData.forEach(function(record) {
		record._id = mongoObjectId();
	});

	mongoClient.connect('mongodb://localhost:27017/contractqab1', function(err, db) {
		db.collection('contracts').drop();
		db.collection('contracts').insert(jsonData, function(err, data) {
			if(err) {throw err}
			db.close();
		});
	});

});