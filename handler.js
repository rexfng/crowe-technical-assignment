'use strict';

module.exports.system_check = (event, context, callback) => {
  let response = {}
      response.statusCode = 200
      response.body = JSON.stringify({
        message: "System is online.",
        input: event
      })
      context.succeed(response)
};

module.exports.fueltypes_restful_get = (event, context, callback) => {
  const PgConnection = require('postgresql-easy');
  const pg = new PgConnection({
    database: process.env.DATABASE,
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD
  });
  pg.getAll('fueltypes')
    .then(function(data){
      console.log(data)
      let response = {}
          response.statusCode = 200
          response.body = JSON.stringify(data)
          context.succeed(response)
    })
    .catch(function(error){
      console.log(error)
      let response = {}
          response.statusCode = 500
          response.body = JSON.stringify(error)
          context.succeed(response)
    })
}

module.exports.fueltypes_restful_post = (event, context, callback) => {
  const PgConnection = require('postgresql-easy');
  const pg = new PgConnection({
    database: process.env.DATABASE,
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    debug: process.env.DEBUG
  });
  pg.query(`INSERT into fueltypes (fuel_type) VALUES ('${JSON.parse(event.body).fuel_type}');`)
    .then(function(data){
      let response = {}
          response.statusCode = 201
          response.body = JSON.stringify({ data })
          context.succeed(response)
    })
    .catch(function(error){
      console.log(error)
      let response = {}
          response.statusCode = 500
          response.body = error
          context.succeed(response)
    })
}

module.exports.fueltypes_restful_getbyid = (event, context, callback) => {
  const PgConnection = require('postgresql-easy');
  const pg = new PgConnection({
    database: process.env.DATABASE,
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    debug: process.env.DEBUG
  });
  pg.getById('fueltypes',event.pathParameters.id)
    .then(function(data){
      let response = {}
          response.statusCode = 201
          response.body = JSON.stringify(data)
          context.succeed(response)
    })
    .catch(function(error){
      console.log(error)
      let response = {}
          response.statusCode = 500
          response.body = JSON.stringify(error)
          context.succeed(response)
    })
}

module.exports.fueltypes_restful_put = (event, context, callback) => {
  const PgConnection = require('postgresql-easy');
  const pg = new PgConnection({
    database: process.env.DATABASE,
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    debug: process.env.DEBUG
  });
  pg.query(`UPDATE fueltypes SET "fuel_type" = '${JSON.parse(event.body)['fuel_type']}' WHERE ID = ${event.pathParameters.id};`)
    .then(function(data){
      let response = {}
          response.statusCode = 204
          response.body = JSON.stringify( data )
          context.succeed(response)
    })
    .catch(function(error){
      console.log(error)
      let response = {}
          response.statusCode = 500
          response.body = JSON.stringify(error)
          context.succeed(response)
    })
}

module.exports.fueltypes_restful_delete = (event, context, callback) => {
  const PgConnection = require('postgresql-easy');
  const pg = new PgConnection({
    database: process.env.DATABASE,
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    debug: process.env.DEBUG
  });
  pg.deleteById('fueltypes',event.pathParameters.id)
    .then(function(data){
      let response = {}
          response.statusCode = 204
          response.body = JSON.stringify(data)
          context.succeed(response)
    })
    .catch(function(error){
      console.log(error)
      let response = {}
          response.statusCode = 500
          response.body = JSON.stringify(error)
          context.succeed(response)
    })
}

module.exports.s3_xls_parser = (event, context, callback) => {
  (async function() {
    try{
      if (event.Records === null) { throw {"name": "EMPTY_RECORD"}}
      const _ = require('lodash')
      const XLSX = require('xlsx');
      const del = require('delete');
      const parsePath = require("parse-path")   
      const PgConnection = require('postgresql-easy');
      const pg = new PgConnection({
        database: process.env.DATABASE,
        host: process.env.HOST,
        user: process.env.USER,
        password: process.env.PASSWORD
      });
      _.each(event.Records, function(e){
        (async function() {
          try{
              const {promisify} = require('util');
              const wget = require('node-wget');
              const wgetAsync = promisify(wget);
              let url = `https://fuel-qoutes.s3.ca-central-1.amazonaws.com/${e.s3.object.key}`
              let excelFile = await wgetAsync({url, dest: "/tmp/"})
              var w = XLSX.readFile(excelFile.filepath);
                w = w.Sheets[w.SheetNames[0]]
              var obj = {}
                obj.vendor = w['B1'].v
                obj.effective_date = w['E1'].w
              let records = XLSX.utils.sheet_to_json(w, {range: 4, header: ["fuel_type", "location", "price", "discount"]})
              let sheet_fueltypes = _.uniqBy(_.map(records, function(e) {return e.fuel_type.toUpperCase()}))
              let fueltypesPromise = _.map(sheet_fueltypes, function(term){
                  return pg.query(`INSERT INTO fueltypes (fuel_type) VALUES ('${term}') ON CONFLICT (fuel_type) DO NOTHING;`)
                })          
              await Promise.all(fueltypesPromise)     
              let fuelmap = await pg.getAll('fueltypes')
              records = _.map(records, function(record){
                let r = {}
                  r.vendor = obj.vendor.toUpperCase()
                  r.effective_date = new Date(obj.effective_date).toISOString()
                  r.price = record.price
                  r.discount = record.discount || 0
                  r.location = record.location
                  r.fuel_type = _.filter(fuelmap, {fuel_type: record.fuel_type.toUpperCase()})[0].id
                return r
              })  
              let record_query = `INSERT INTO fuelprices (vendor, effective_date, price, discount, location, fuel_type) VALUES` + _.map(records, function(r){
                return `('${r.vendor}','${r.effective_date}', ${r.price}, ${r.discount}, '${r.location}', ${r.fuel_type})`
              }).join(',') + ";"
              await pg.query(record_query)
              await del.promise([`./tmp/${parsePath(url).pathname}`])
              return ;
            }
            catch(error){
              console.log(error)
              return;
            }
        })()
      })
    }  
    catch(error){
      console.log(error)
      const _ = require('lodash')
      const del = require('delete');
      const parsePath = require("parse-path")   
      let delPromises = _.map(event.Records, function(e){
        let url = `https://fuel-qoutes.s3.ca-central-1.amazonaws.com/${e.s3.object.key}`
        return del.promise([`./tmp/${parsePath(url).pathname}`])
      })
      Promise.all(delPromises)
        .then(function(success){
          console.log('success', success)
          return;
        })
        .catch(function(error){
          console.log('error', error)
          return;
        })
    }
  })()
}