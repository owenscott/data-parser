var fs = require('fs');

var test = fs.readFileSync('./output/draft-output-for-anjesh-7-30.json').toString();

test = JSON.parse(test);

console.log(test.length);