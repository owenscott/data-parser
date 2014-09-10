module.exports = function(file) {
	return {
		version: file.substr(0,2),
		coder: file.substr(3,file.length - 7)
	}
}