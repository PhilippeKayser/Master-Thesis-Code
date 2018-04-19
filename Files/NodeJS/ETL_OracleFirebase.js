//Requires
var fs = require('fs');
var async = require('async');
var admin = require('firebase-admin');
var oracledb = require('oracledb');
var ftpClient = require('ftp-client');
var database = require('./database.js')

//Firebase ServiceAccoutn
var serviceAccount = require("./configData/serviceclient.json");

//Initialise Storage
var gcs = require('@google-cloud/storage')({
	projectID:'****',
	keyFilename: './configData/serviceclient.json'
});
	//Get the Storage Bucket
var bucket = gcs.bucket('****');

//Initialize Realtime Database
admin.initializeApp({
	credential: admin.credential.cert(serviceAccount),
	databaseURL: "****"
});
	//Get the Database
var db = admin.database();

//ftpClient
var ftpConfig = {
	host: '****',
	user:'****',
	password:'****'
};

client = new ftpClient(ftpConfig, {overwrite:'all'});

//DWH Configuration
var dwhConfig = {
	user:"dwh",
	password:"dwh",
	connectString:"localhost/db12c"
}

//Variables
var queries = [];
var sessionsToAnalyze = [];

//Interval Vars
var minutes = 0.5;
var intervalTime = minutes * 60 * 1000; 
var num = 1;

//Execute Run Method
run();

//Run Method
async function run() {
	var currentDate = Date.now();

	console.log("Creating Connection-Pool...");
	await database.createPool(dwhConfig);
	console.log("Done");

	console.log("Checking if Files are available in Awaiting...");
	getQueriesFromFolder("awaiting");

	if(Object.keys(queries).length > 0)
	{
		await treatQueries(queries);
		console.log("Available Files treated.");
	}
	console.log("Done");

	console.log("Checking Firebase for changes...");
	await db.ref("Sessions").once('value').then(function(datasnapshot){
		var sessJSON = datasnapshot.toJSON();
		for(let k in sessJSON)
		{	
			if(sessJSON[k].Active === 'True' || (sessJSON[k].Active === 'False' && sessJSON[k].ClosingTransmitted == null))
			{
				sessionsToAnalyze.push(k);
			}
		}
		
	});
	await analyzeSessions(sessionsToAnalyze);
	sessionsToAnalyze = [];

	console.log("Adding Listeners for Firebase...");
	db.ref("Sessions").on("child_changed", (snapshot, prevChildKey) => {
		console.log(snapshot.key+" changed.");
		sessionsToAnalyze.push(snapshot.key);
	});
	
	db.ref("Orders").on("child_changed", 
		function(snapshot, prevChildKey) 
		{
			console.log(snapshot.key+" changed.");
			sessionsToAnalyze.push(snapshot.key);
		}
	);

	var initialSessionLoad = true;
	db.ref("Orders").on("child_added", (snapshot, prevChildKey) => {
		if(!initialSessionLoad)
		{
			console.log("Order for "+snapshot.key+" added.");
			sessionsToAnalyze.push(snapshot.key);
		}
	});

	db.ref("Orders").once('value', (snapshot) => {
		if(initialSessionLoad)
			initialSessionLoad = false;
	});

	db.ref("Payments").on("child_changed", 
		function(snapshot, prevChildKey) 
		{
			console.log(snapshot.key+" changed.");
			sessionsToAnalyze.push(snapshot.key);
		}
	);
	console.log("Listeners Created");

	console.log("Setting Interval...")	
	setInterval(async () => {
		getQueriesFromFolder("awaiting");

		if(Object.keys(queries).length > 0)
			await treatQueries(queries);

		await analyzeSessions(sessionsToAnalyze);
		//uploadMenuFile();
		sessionsToAnalyze = [];
	}, intervalTime);
	console.log("Interval Created");
}

function uploadMenuFile() {
	bucket.upload('menu/menuFile.json',{
	destination:'menu/menuFile.json',
	public:true
	}, (err, file) => {
		if(err) {
			console.log(err);
			return;
		}
		console.log("Upload to Firebase successful");
	});

	client.connect(() => {
		client.upload(['./menu/menuFile.json'], '/public_html/',{baseDir:'menu', overwrite:'all'},(result) => {
			console.log("Upload to Server successful");
		});
	});


}

async function transferPayment(session, timestamp, paymentJSON) {
	var pf;

	return new Promise(async (resolve, reject) => {
		var dataQuery = 	"BEGIN "+
							"SELECT dd.dim_date_id, dt.dim_time_id, ds.dim_sessions_id, -10, dpm.dim_payment_methods_id, -404 "+
							"INTO :p_date_id, :p_time_id, :p_sess_id, :p_emp_id, :p_dpm_id, :p_cust_id " +
							"FROM dim_date dd, dim_time dt, dim_sessions ds, dim_payment_methods dpm "+
							"WHERE dd.year = "+timestamp.substr(0,4)+" AND "+
									"dd.month = "+timestamp.substr(5,2)+" AND "+
									"dd.day = "+timestamp.substr(8,2)+" AND "+
									"dt.hour = "+timestamp.substr(11,2)+" AND "+
									"dt.minute = "+timestamp.substr(14,2)+" AND "+
									"ds.session_guid = UPPER('"+session+"') AND "+
									"dpm.payment_method IN ('"+paymentJSON.PaymentMethod+"'); "+
						"END;";

		var bindParams = 
		{
			p_date_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_time_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_sess_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_emp_id  : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_dpm_id  : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_cust_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT}
		};

		var binds = {};

		await database.executeQuery(dataQuery,bindParams, {}).then(function(result){
			binds = result.outBinds;
		}).catch((err) => {
			console.error(err);
			reject(err);
		});

		binds.p_pf_guid = {type:oracledb.BINARY, dir:oracledb.BIND_OUT};

		var pfQuery = 	"INSERT INTO payment_fact(price_paid, discount, bonus_points, dim_date_id, dim_time_id, dim_sessions_id, dim_employees_id, dim_payment_methods_id, dim_customers_id) "+
						"VALUES("+paymentJSON.TotalAmount+","+paymentJSON.Discount+","+paymentJSON.TotalAmount*0.05+", :p_date_id, :p_time_id, :p_sess_id, :p_emp_id, :p_dpm_id, :p_cust_id) RETURNING pf_guid INTO :p_pf_guid";

		await database.executeQuery(pfQuery,binds, {}).then(function(result){
			console.log("Created Payment Fact Element with ID "+result.outBinds.p_pf_guid[0]);
			binds.p_pf_guid = result.outBinds.p_pf_guid[0];
			//Schreibe GUID
		}).catch((err) => {
			console.error(err);
			reject(err);
		});
		resolve(binds.p_pf_guid);
	});
}

async function transferOrder(session, timestamp, orderJSON){
	var isf;

	return new Promise(async (resolve, reject) => {
		var dataQuery = 	"BEGIN "+
							"SELECT dd.dim_date_id, dt.dim_time_id, pi.prod_items_id, -404, ds.dim_sessions_id, -10 "+
							"INTO :p_date_id, :p_time_id, :p_prod_id, :p_cust_id, :p_sess_id, :p_emp_id " +
							"FROM dim_date dd, dim_time dt, prod_items pi, dim_sessions ds, prod_items_history pih "+
							"WHERE dd.year = "+timestamp.substr(0,4)+" AND "+
									"dd.month = "+timestamp.substr(5,2)+" AND "+
									"dd.day = "+timestamp.substr(8,2)+" AND "+
									"dt.hour = "+timestamp.substr(11,2)+" AND "+
									"dt.minute = "+timestamp.substr(14,2)+" AND "+
									"pi.original_item_id = "+orderJSON.ItemKey+" AND "+
									"pi.price = "+orderJSON.PriceKey+" AND "+
									"ds.session_guid = UPPER('"+session+"') AND "+
									"pih.prod_items_id = pi.prod_items_id AND "+
									"pih.end_date > sysdate; "+
						"END;";

		var bindParams = 
		{
			p_date_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_time_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_prod_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_cust_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_sess_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT},
			p_emp_id  : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT}
		};

		var binds = {};

		await database.executeQuery(dataQuery,bindParams, {}).then(function(result){
			binds = result.outBinds;
		}).catch((err) => {
			console.error(err);
			reject(err);
		});

		binds.p_isf_guid = {type:oracledb.BINARY, dir:oracledb.BIND_OUT};

		var isfQuery = "INSERT INTO item_sales_fact(quantity_sold, price, dim_date_id, dim_time_id, prod_items_id, dim_employees_id, dim_sessions_id, dim_customers_id) "+
						"VALUES ("+orderJSON.Amount+","+orderJSON.PriceKey+",:p_date_id, :p_time_id, :p_prod_id, :p_emp_id, :p_sess_id, :p_cust_id) RETURNING isf_guid INTO :p_isf_guid";
								
		await database.executeQuery(isfQuery,binds, {}).then(function(result){
			console.log("Created Sales Fact Element with ID "+result.outBinds.p_isf_guid[0]);
			binds.p_isf_guid = result.outBinds.p_isf_guid[0];
			//Schreibe GUID
		}).catch((err) => {
			console.error(err);
			reject(err);
		});

		if(orderJSON.Selections != null) {
			for(let s in orderJSON.Selections) {
				let current = orderJSON.Selections[s];
				var disfQuery = "INSERT INTO detailed_item_sales_fact(isf_guid, dim_date_id, dim_time_id, prod_items_id, dim_employees_id, dim_sessions_id, dim_customers_id, prod_custom_items_id, dim_selections_id, dim_extra_items_id) "+
				"SELECT :p_isf_guid,:p_date_id,:p_time_id,:p_prod_id,:p_emp_id,:p_sess_id,:p_cust_id,-1,ds.dim_selections_id,-1 "+
				"FROM dim_selections ds "+
				"WHERE ds.original_selg_id = "+current.GroupKey+" AND "+
				"ds.original_sel_id = "+current.SelectionKey;

				database.executeQuery(disfQuery,binds,{}).then((result) => {
					console.log("Inserted Detail");
				}).catch((err) => {
					console.error(err);
					reject(err);
				});

			}
		}

		if(orderJSON.Customizations != null) {
			for(let c in orderJSON.Customizations) {
				let current = orderJSON.Customizations[c];
				var disfQuery = "INSERT INTO detailed_item_sales_fact(isf_guid, dim_date_id, dim_time_id, prod_items_id, dim_employees_id, dim_sessions_id, dim_customers_id, prod_custom_items_id, dim_selections_id, dim_extra_items_id) "+
				"SELECT :p_isf_guid,:p_date_id,:p_time_id,:p_prod_id,:p_emp_id,:p_sess_id,:p_cust_id,pci.prod_custom_items_id,-1,-1 "+
				"FROM prod_custom_items pci "+
				"WHERE pci.original_item_id = "+current.CustomItemKey;

				database.executeQuery(disfQuery,binds,{}).then((result) => {
					console.log("Inserted Detail");
				}).catch((err) => {
					console.error(err);
					reject(err);
				});

			}
		}
		resolve(binds.p_isf_guid);
	});
}

async function analyzeSessions(sessionArray){
	
	var ordersReferences = [];
	var paymentsReferences = [];

	//Get necessary references
	for(let k in sessionArray){
		await db.ref("Sessions/"+sessionArray[k]).once('value').then(async function(datasnapshot){
			
			let sessQuery = "BEGIN SELECT dim_sessions_id INTO :p_dim_sessions_id FROM dim_sessions WHERE session_guid = UPPER('"+sessionArray[k]+"'); END;";
			
			await database.executeQuery(sessQuery, {p_dim_sessions_id : {type:oracledb.NUMBER, dir:oracledb.BIND_OUT}}, {}).then(async function(result){
				console.log("Session "+sessionArray[k]+" exists in DWH.");
			}).catch(async function(err){
				console.log("Session "+sessionArray[k]+" doesn't exist. Adding to DWH.");
				let insQuery = "INSERT INTO dim_sessions(session_guid, session_start) VALUES (UPPER('"+sessionArray[k]+"'), to_date('"+datasnapshot.val().Created+"','YYYY-MM-DD HH24:MI:SS'))";

				await database.executeQuery(insQuery,{},{}).then(function(result){
					console.log("Insert was successful.");
				}).catch(function(err){
					console.error(err);
				});
			});


			if(datasnapshot.val().ClosingTransmitted == null)
			{
				ordersReferences.push(db.ref("Orders/"+sessionArray[k]));
				if(datasnapshot.val().Active === 'False') {
					paymentsReferences.push(db.ref("Payments/"+sessionArray[k]));
				}

				var expDate = Date.parse(datasnapshot.val().Expires);
				//Quick fix from GMT+1
				var nowDate = new Date(Date.now()+60*60*1000);

				if(expDate < nowDate && datasnapshot.val().Active === 'True') {
					console.log("Session "+sessionArray[k]+" Has expired.");

					let updQuery = "UPDATE dim_sessions SET session_closed = to_timestamp('"+new Date(nowDate).toISOString().replace(/T/,' ').replace(/\..+/,'')+"','YYYY-MM-DD HH24:MI:SS'), session_status = 'E' WHERE session_guid = UPPER('"+sessionArray[k]+"')";
					await database.executeQuery(updQuery,{},{}).then((result) => {
						console.log("Session was set to expired.");
						db.ref("Sessions/"+sessionArray[k]).child("Active").set("False");
						db.ref("Sessions/"+sessionArray[k]).child("Closed").set(new Date(nowDate).toISOString().replace(/T/,' ').replace(/\..+/,''));
						db.ref("Sessions/"+sessionArray[k]).child("ClosingTransmitted").set("True");
					}).catch((err) => {
						console.error(err);
					});
				} else if(expDate >= nowDate && datasnapshot.val().Active === 'False') {
					console.log("Session "+sessionArray[k]+" was closed.");

					let updQuery = "UPDATE dim_sessions SET session_closed = to_timestamp('"+new Date(nowDate).toISOString().replace(/T/,' ').replace(/\..+/,'')+"','YYYY-MM-DD HH24:MI:SS'), session_status = 'P' WHERE session_guid = UPPER('"+sessionArray[k]+"')";
					await database.executeQuery(updQuery,{},{}).then((result) => {
						console.log("Session was set to paid.");
						db.ref("Sessions/"+sessionArray[k]).child("Closed").set(new Date(nowDate).toISOString().replace(/T/,' ').replace(/\..+/,''));
						db.ref("Sessions/"+sessionArray[k]).child("ClosingTransmitted").set("True");
					}).catch((err) => {
						console.error(err);
					});
				}
			}

			
		});
	}

	//Work with orders references
	for(let k in ordersReferences){
		await ordersReferences[k].once('value').then(async (datasnapshot) => {
			var currentOrderJSON = datasnapshot.toJSON();
			for(let timestamp in currentOrderJSON) {
				for(let line in currentOrderJSON[timestamp]) {
					if(currentOrderJSON[timestamp][line].Transferred === 'False' || currentOrderJSON[timestamp][line].Transferred == null) {
						await transferOrder(sessionArray[k], timestamp, currentOrderJSON[timestamp][line]).then((isf)=>{
							console.log("Order Executed "+isf);
							db.ref("Orders/"+sessionArray[k]+"/"+timestamp+"/"+line).child("Transferred").set('True');
							db.ref("Orders/"+sessionArray[k]+"/"+timestamp+"/"+line).child("TransferGUID").set(isf);
						}).catch((err) => {
							console.error(err);
						});
					}
				}
			}
		});
	}

	//work with payments references
	for(let k in paymentsReferences)
	{
		await paymentsReferences[k].once('value').then(async (datasnapshot) => {
			var currentPayment = datasnapshot.toJSON();
			for(let timestamp in currentPayment) {
				//console.log(sessionArray[k]);
				await transferPayment(sessionArray[k], timestamp, currentPayment[timestamp]).then((pf) => {
					console.log("Payment received "+pf);
					db.ref("Payments/"+sessionArray[k]+"/"+timestamp).child("PaymentGUID").set(pf);
				}).catch((err) => {
					console.error(err);
				});
			}

		});

		//console.log(paymentsReferences[k].toJSON());
	}
	sessionArray = [];
}

function getQueriesFromFolder(folder) {
	var files = fs.readdirSync('./'+folder);
	async.eachSeries(files, function(file, cb){
		var q = getQuery(folder,file);
		//console.log("Query: "+q);

		if(q != null)
			queries[file.substr(0,18)] = q;
		
		fs.rename("./"+folder+"/"+file, "./treating/"+file, function(err){
			if(err){
				console.error(err);
			}
		});

		cb();
	});
}

async function treatQueries(queries){
	

	for(const [key, value] of Object.entries(queries)) {
		var newID;
		await database.executeQuery(value, {}, {}).then(function(result){
			console.log(num+": Query Successfully exectued;");
			fs.unlink("treating/"+key+".json", (err) => {
				if(err) {
					console.error(err);
				}
			});
		}).catch(function(err){
			console.log(num+": Failed to execute Query "+value);
			fs.rename('treating/' + key+".json", 'failed/' + key+".json", function (err) { 
				if (err) { 
					console.error(err); 
				} 
				log.file = "Moved to failed";
			});
			console.error(err);
		});
		num++;

		delete queries[key];
	}	

	uploadMenuFile();
}

function getQuery(folder, file) {
	var contents;
	//Contains parsed JSON-Object
	var json;
	try {
		contents = fs.readFileSync(folder+'/'+file, 'utf-8');
	} catch(err) {
		console.log(myNumber+": %%% ERROR %%% Content of "+file+" could not be read.");
		fs.rename('awaiting/' + file, 'failed/' + file, function (err) { if (err) { console.error(err); return; } log.file ="Moved to failed";});
		return null;
	}

	try {
		json = JSON.parse(contents);
	} catch (err) {
		//Error if Content is not JSON.
		console.log(myNumber+": %%% ERROR %%% Content of "+file+" was not a correct JSON-File.");
		fs.rename('awaiting/' + file, 'failed/' + file, function (err) { if (err) { console.error(err); return; } log.file ="Moved to failed";});
		return null;
	}


	//Query Var
	var query = '';
	var putSeparator;
	var initial = true;

	//FALLS INSERT
	if (json.action == 'INSERT') {
		var columns = '';
		var values = '';
		if(json.targetDimension != 'DIM_CUSTOMERS'){
			columns = 'FROM_DATE,';
			values = 'to_date(\''+json.timestamp+'\',\'YYYY.MM.DD HH24:MI:SS\'),';
		}
		query += 'INSERT INTO VW_' + json.targetDimension + '(';

		initial = true;
		for(var pKey in json.primaryKey) {
			if(!initial){
				columns += ',';
				values += ',';
			}
			columns += pKey;

			if(json.primaryKey[pKey] == null)
				values += 'null';
			else
				values += '\''+json.primaryKey[pKey].replace(/'/g, "''")+'\'';
			if(initial)
				initial = false;
		}

		for(var val in json.values) {
			
			columns += ',';
			values += ',';
			
			columns += val;
			if(json.values[val] == null)
				values += 'null';
			else
				if(val != 'birthdate')
					values += '\''+json.values[val].replace(/'/g, "''")+'\'';
				else
					values += 'to_date(\''+json.values[val].replace(/'/g, "''")+'\',\'DD.MM.YYYY\')';

		}

		query += columns + ') VALUES (' + values + ')';		
	}

	//FALLS DELETE
	else if (json.action == 'DELETE') {
		putSeparator = false;

		query += 'DELETE FROM VW_' + json.targetDimension + ' WHERE ';

		for(var pKey in json.primaryKey) {
			if (putSeparator) {
				query += ' AND ';
			}

			if(json.primaryKey[pKey] == null)
				query += pKey + ' IS null';
			else
				query += pKey +' = \''+json.primaryKey[pKey].replace(/'/g, "''")+'\'';

			if(!putSeparator)
			{
				putSeparator = true;
			}
		}

		if(json.targetDimension != 'DIM_CUSTOMERS')
			query += ' AND TO_TIMESTAMP(\'' + json.timestamp + '\',\'YYYY.MM.DD HH24:MI:SS\') BETWEEN from_date AND end_date';

	}
	

	
	//FALLS UPDATE
	else if (json.action == 'UPDATE') {

		query += 'UPDATE VW_' + json.targetDimension + ' SET ';
		if(json.targetDimension != 'DIM_CUSTOMERS')
			query += 'FROM_DATE = TO_DATE(\''+json.timestamp+'\',\'YYYY.MM.DD HH24:MI:SS\'),';
		initial = true;
		for (var val in json.values) {
			if(!initial)
				query += ',';
			

			if(json.values[val] == null)
				query += val + ' IS null';
			else
				if(val != 'birthdate')
					query += val + ' = \''+json.values[val].replace(/'/g, "''")+'\'';
				else
					query += val + ' = to_date(\''+json.values[val].replace(/'/g, "''")+'\',\'DD.MM.YYYY\')';
			if(initial)
				initial = false;
		}

		query += ' WHERE ';

		putSeparator = false;
		for(var pKey in json.primaryKey) {
			if (putSeparator) {
				query += ' AND ';
			}

			if(json.primaryKey[pKey] == null)
				query += pKey + ' = null';
			else
				query += pKey +' = \''+json.primaryKey[pKey].replace(/'/g, "''")+'\'';

			if(!putSeparator)
			{
				putSeparator = true;
			}
		}
		
		if(json.targetDimension != 'DIM_CUSTOMERS')
			query += ' AND TO_TIMESTAMP(\'' + json.timestamp + '\',\'YYYY.MM.DD HH24:MI:SS\') BETWEEN from_date AND end_date';
	}


	return query;
}