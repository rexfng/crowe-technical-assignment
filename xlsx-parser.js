(async function() {
	try{
		// const pg = new PgConnection({
		//   database: process.env.DATABASE,
		//   host: process.env.HOST,
		//   user: process.env.USER,
		//   password: process.env.PASSWORD
		// });
		const {promisify} = require('util');
		const wget = require('node-wget');
			  wgetAsync = promisify(wget);
		const _ = require('lodash')
		const XLSX = require('xlsx');
		const PgConnection = require('postgresql-easy');
		const del = require('delete');
		const parsePath = require("parse-path")		
		let url = "https://fuel-qoutes.s3.ca-central-1.amazonaws.com/Chevron%2BFuel%2BPrices.xlsx"
		const pg = new PgConnection({
			database: "fuelservices",
			host: "fuelservices.c55btmimc6xf.ca-central-1.rds.amazonaws.com",
			user: "userfuel",
			password: "UserPassw0rd"
		});
		let excelFile = await wgetAsync(url)
		var w = XLSX.readFile(excelFile.filepath);
			w = w.Sheets[w.SheetNames[0]]
		var obj = {}
			obj.Vendor = w['B1'].v
			obj.effective_date = w['E1'].w
		let records = XLSX.utils.sheet_to_json(w, {range: 4, header: ["fuel_type", "location", "price", "discount"]})
		let sheet_fueltypes = _.uniqBy(_.map(records, function(e) {return e.fuel_type.toUpperCase()}))
			fueltypesPromise = _.map(sheet_fueltypes, function(term){
				return pg.query(`INSERT INTO fueltypes (fuel_type) 
					VALUES ('${term}') ON CONFLICT (fuel_type) DO NOTHING;`)
			})					
			await Promise.all(fueltypesPromise)			
			let fuelmap = await pg.getAll('fueltypes')
			records = _.map(records, function(record){
				let r = {}
					r.Vendor = obj.Vendor.toUpperCase()
					r.effective_date = new Date(obj.effective_date).toISOString()
					r.price = record.price
					r.discount = record.discount || 0
					r.location = record.location
					r['Fuel type'] = _.filter(fuelmap, {fuel_type: record.fuel_type.toUpperCase()})[0].id
				return r
			})	
			record_query = `INSERT INTO fuelprices (vendor, effective_date, price, discount, location, fuel_type) VALUES` + _.map(records, function(r){
				return `('${r.Vendor}','${r.effective_date}', ${r.price}, ${r.discount}, '${r.location}', '${r['Fuel type']}')`
			}).join(',') + ";"
			console.log(record_query)
			await pg.query(record_query)
			await del.promise([parsePath(url).pathname])
	}	
	catch(error){
		const {promisify} = require('util');
		const wget = require('node-wget');
			  wgetAsync = promisify(wget);
		const _ = require('lodash')
		const XLSX = require('xlsx');
		const PgConnection = require('postgresql-easy');
		const del = require('delete');
		const parsePath = require("parse-path")		
		let url = "https://fuel-qoutes.s3.ca-central-1.amazonaws.com/Chevron%2BFuel%2BPrices.xlsx"
		console.log(parsePath(url).pathname)
		let delfile = await del.promise([`.${parsePath(url).pathname}`])
		console.log('delfile', delfile)
		console.log(error)
	}
})()