var _ = require('underscore');


var problemRecords = [
	'KDC 069/070',
	'2069/070(MaJaVK-LiV-001)',
	'KGA-069/070-TC-10',
	'KGA-069/070-TM-07',
	'KGA-069/070-TM-12',
	'NEA/ES/BGHEP-01',
	'NEA/ES/BGHEP-01',
	'NEA/ES/BGHEP-01',
	'MKHPS-070/071-ES-Q3',
	'NEA/BDC',
	'NEA/BDC',
	'069/070 -MKHPS-ES-T1'
]

module.exports = function(record) {
	if (record.meta.status === 'open') {
		return true
	}
	else if (record.data.keyValuePairs.merge.NOTES && record.data.keyValuePairs.merge.NOTES.value === 'NPR') {
		return true;
	}
	else if (record.data.keyValuePairs.merge.CONTRACTTYPE && record.data.keyValuePairs.merge.CONTRACTTYPE.value.indexOf('NULL') > -1) {
		return true;
	}
	else if (record.data.keyValuePairs.merge.CONTRACTNUMBER && record.data.keyValuePairs.merge.CONTRACTNUMBER.value.indexOf('NULL') > -1) {
		return true;
	}
	else if (record.data.keyValuePairs.merge.PROJECTNAME && record.data.keyValuePairs.merge.PROJECTNAME.value === 'FALSE') {
		return true;
	}
	else if (record.data.keyValuePairs.merge.CONTRACTTYPE && record.data.keyValuePairs.merge.CONTRACTTYPE.value.indexOf('Construction') > -1 ) {
		return true;
	}
	else if (_.contains(problemRecords, record.data.keyValuePairs.merge['TENDERNOTICENUMBER'].cleanValue) || _.contains(problemRecords, record.data.keyValuePairs.merge['TENDERNOTICENUMBER'].value)){
		console.log('problem record');
		return true
	}
	else {
		return false;
	}
}