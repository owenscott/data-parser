var fs = require('fs'),
	_ = require('underscore'),
	csvParse = require('csv-parse'),
	async = require('async'),
	mongodb = require('mongodb'),
	createHash = require('./src/createMd5FromJson.js'),
	exclude = require('./src/exclude.js');

var	data = [];

var conf = JSON.parse(fs.readFileSync('./conf.json').toString());

var scrapedFields = [
	'STATUS',
	'ISSUER',
	'PUBLICATIONDATE',
	'PUBLISHEDIN',
	'DOCUMENTPURCHASEDEADLINE',
	'SUBMISSIONDEADLINE',
	'OPENINGDATE',
	'URL'
]


var typeFields = [
	'GOODS',
	'SERVICES',
	'CONSTRUCTION',
	'MAINTENANCE'
]










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
	mongodb.connect('mongodb://localhost:27017/' + dbName, function(err, db) {
		
		if (err) throw new Error(err);

		db.collection('contracts').find().toArray(function(err, rawData) {


			rawData.forEach(function(record) {

				exclude(record);
				

				if (!exclude(record)) {

					var tempRecord = {},
						tempKvps = {},
						locations;

					var tempType = '';

					// console.log(record.data.keyValuePairs.merge['TENDERNOTICENUMBER'].value)

		


					//translate data to new format

					_.keys(record.data.keyValuePairs.merge).forEach(function(key) {
						if ((record.data.keyValuePairs.merge[key].cleanValue || record.data.keyValuePairs.merge[key].value) && mapKey(key)) {
							tempKvps[mapKey(key)] = record.data.keyValuePairs.merge[key].cleanValue || record.data.keyValuePairs.merge[key].value;
						}
					})

					//clean data

					//add desired scraped data

					scrapedFields.forEach(function(field) {
						if (record.scraped[field]){
							tempRecord[mapKey(field)] = record.scraped[field]
						}

					})


					locations = _.pluck(record.data.arrays.merge.locations,'value');

					// console.log(locations);

					//more cleaning data
					
					if (!_.isEmpty(tempKvps)) {
						_.extend(tempRecord, tempKvps);
					}

					if (locations.length > 0) {
						tempRecord.locations = _.clone(locations);
					}

					if (tempRecord['contractIssuer']) {
						tempRecord['contractIssuer'] = tempRecord['contractIssuer'].replace('Issued by\\u0009','')
					}

					if (tempRecord['contractType'] && tempRecord['contractType'] === 'MULTIPLE TYPES') {
						delete tempRecord['contractType'];
					}


					if(tempRecord.contractType) {
						tempType = tempRecord.contractType;
					}

					tempRecord.contractType = [];

					if (tempType && tempType !== 'MULTIPLE TYPES') {
						if (tempType === 'Goods and Services') {
							tempRecord.contractType.push('GOODS');
							tempRecord.contractType.push('SERVICES');
						}
						else if (tempType === 'Maintainence') {
							tempRecord.contractType.push('MAINTENANCE')
						}
						else if (tempType === 'Service') {
							tempRecord.contractType.push('SERVICES')
						}
						else {
							tempRecord.contractType.push(tempType);
						}
					}

					typeFields.forEach(function(type) {
						// console.log(record.data.keyValuePairs.merge);
						if (record.data.keyValuePairs.merge[type] && record.data.keyValuePairs.merge[type].value === 'TRUE') {
							tempRecord.contractType.push(type);
						}
					})


					tempRecord.id = createHash(_.omit(record.scraped, ['ID', 'hash']));

					data.push(_.clone(tempRecord));

				}

		


			})

			db.close();
			callback();


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

			console.log(data.length);

			var uniqueData = _.uniq(data, function(d) {return d.id});

			fs.writeFileSync('./temp.json', JSON.stringify(uniqueData))

			console.log(uniqueData.length)


		}
	);




})




