var fs = require('fs'),
	_ = require('underscore'),
	csvParse = require('csv-parse'),
	async = require('async'),
	mongodb = require('mongodb');

var	data = [];

var conf = JSON.parse(fs.readFileSync('./conf.json').toString());


var mapKey = function(key) {

	var mapping = {
		TENDERNOTICENUMBER: 'tenderNoticeNumber',
		STATUS: 'status',
		URL: 'documentURL',
		CONTRACTDETAILS: 'contractDetails',
		ISSUER: 'contractIssuer',
		PUBLICATIONDATE: 'publicationDate',
		PUBLISHEDIN: 'publicationVenue',
		DOCUMENTPURCHASEDEADLINE: 'documentPurchaseDeadline',
		SUBMISSIONDEADLINE: 'bidSubmissionDeadline',
		OPENINGDATE: 'bidOpeningDate',
		CONTRACTNAME: 'contractName',
		CONTRACTDESCRIPTION: 'contractDescription',
		COSTESTIMATE: 'costEstimate',
		ESTIMATECURRENCY: 'costEstimateCurrency',
		DATASOURCE: 'dataSource',
		CONTRACTNUMBER: 'contractNumber',
		CONTRACTTYPE: 'contractType',
		PROJECTNAME: 'projectName',
		PROJECTNUMBER: 'projectNumber',
		PROJECTFUNDER: 'projectFunder'
	}

	return mapping[key] || undefined;
}


// ----------------------------------------------
// |       PROCESS RAW DATA TO BETTER JSON      |
// ----------------------------------------------

async.each(conf.previousContractDBs,
function(dbName, callback) {
	mongodb.connect('mongodb://localhost:27014/' + dbName, function(err, db) {
		
		if (err) throw new Error(err);

		db.collections('contracts').find().toArray(function(err, arr) {
			console.log('foo');
			process.exit();


			rawData.forEach(function(record) {
				
				var tempRecord = {},
					tempKvps = {},
					locations;


				console.log(record);
				process.exit();

				//translate data to new format

				_.keys(record.data.keyValuePairs.merge).forEach(function(key) {
					if (record.data.keyValuePairs.merge[key].match && mapKey(key)) {
						tempKvps[mapKey(key)] = record.data.keyValuePairs.merge[key].value;
					}
				})

				//clean data

				if (tempKvps['contractIssuer']) {
					tempKvps['contractIssuer'] = tempKvps['contractIssuer'].replace('Issued by\\u0009','')
				}

				if (tempKvps['contractType'] && tempKvps['contractType'] === 'MULTIPLE TYPES') {
					delete tempKvps['contractType'];
				}

				locations = _.pluck(record.data.arrays.merge.locations,'value');

				// console.log(locations);

				//more cleaning data
				
				if (!_.isEmpty(tempKvps)) {
					_.extend(tempRecord, tempKvps);
				}

				if (locations.length > 0) {
					tempRecord.locations = _.clone(locations);
				}

				data.push(_.clone(tempRecord));

			})


		})

	})
},
function(){

	// ----------------------------------------------
	// |            PROCESS RAW GADM DATA           |
	// ----------------------------------------------

	var gadmRaw = [],
		gadm = [],
		i = 0;

	gadmRaw[0] = fs.readFileSync('./ref/gadm-adm1.csv').toString();
	gadmRaw[1] = fs.readFileSync('./ref/gadm-adm2.csv').toString();
	gadmRaw[2] = fs.readFileSync('./ref/gadm-adm3.csv').toString();
	gadmRaw[3] = fs.readFileSync('./ref/gadm-adm4.csv').toString();

	async.eachSeries(gadmRaw, function(csv, callback) {
		csvParse(csv, {columns:true}, function(err, data) {
			gadm[i] = data;
			i++;
			callback();
		})
	},
		function( ){

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

			// //add gadm data to locations and convert to flat csv output
			data.forEach(function(d) {


				//add gadm data
				d.locations = _.map(d.locations, function(location) {
					var locArr = [];
					var splitLocations = location.split('|');

					for (var i = 0; i < splitLocations.length; i++) {
						if(!_.isEmpty(_.findWhere(gadm[i], {name:splitLocations[i]}))) {
							locArr.push(_.findWhere(gadm[i], {name:splitLocations[i]}))
						}

					}

					return _.flatten(locArr);
					
				})

				d.locations = _.flatten(d.locations);
				d.locations = _.uniq(d.locations, function(d) {return JSON.stringify(d)});

				if (d.locations.length <= 0) {
					delete d.locations;
				}

				if (_.isEmpty(d)) {
					delete d;
				}
		
			})


		}
	);




})




