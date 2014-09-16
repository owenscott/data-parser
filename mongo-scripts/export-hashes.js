db.contracts.find({},{}).forEach(function(contract) {
	print (contract.scraped.hash);
})