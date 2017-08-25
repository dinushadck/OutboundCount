/**
 * Created by dinusha on 8/25/2017.
 */

var config = require('config');
var util = require('util');
var mongoose = require('mongoose');
var restify = require('restify');
var moment = require('moment');
var dbModel = require('dvp-dbmodels');
var User = require('dvp-mongomodels/model/User');

var mongoip=config.Mongo.ip;
var mongoport=config.Mongo.port;
var mongodb=config.Mongo.dbname;
var mongouser=config.Mongo.user;
var mongopass = config.Mongo.password;
var mongoreplicaset= config.Mongo.replicaset;

var companyId = config.CompanyId;
var tenantId = config.TenantId;

var connectionstring = '';

console.log(mongoip);

mongoip = mongoip.split(',');

console.log(mongoip);

if(util.isArray(mongoip)){

    if(mongoip.length > 1){

        mongoip.forEach(function(item){
            connectionstring += util.format('%s:%d,',item,mongoport)
        });

        connectionstring = connectionstring.substring(0, connectionstring.length - 1);
        connectionstring = util.format('mongodb://%s:%s@%s/%s',mongouser,mongopass,connectionstring,mongodb);

        if(mongoreplicaset){
            connectionstring = util.format('%s?replicaSet=%s',connectionstring,mongoreplicaset) ;
        }
    }else{

        connectionstring = util.format('mongodb://%s:%s@%s:%d/%s',mongouser,mongopass,mongoip[0],mongoport,mongodb)
    }

}else{

    connectionstring = util.format('mongodb://%s:%s@%s:%d/%s',mongouser,mongopass,mongoip,mongoport,mongodb)
}

console.log(connectionstring);

mongoose.connect(connectionstring,{server:{auto_reconnect:true}});


mongoose.connection.on('error', function (err) {
    console.error( new Error(err));
    mongoose.disconnect();

});

mongoose.connection.on('opening', function() {
    console.log("reconnecting... %d", mongoose.connection.readyState);
});


mongoose.connection.on('disconnected', function() {
    console.error( new Error('Could not connect to database'));
    mongoose.connect(connectionstring,{server:{auto_reconnect:true}});
});

mongoose.connection.once('open', function() {
    console.log("Connected to db");

});


mongoose.connection.on('reconnected', function () {
    console.log('MongoDB reconnected!');
});


process.on('SIGINT', function() {
    mongoose.connection.close(function () {
        console.log('Mongoose default connection disconnected through app termination');
        process.exit(0);
    });
});

var server = restify.createServer();

var queryExecute = function(qDate, resId, sipUser)
{
    var query1 = "SELECT COUNT (*), SUM(\"BillSec\") FROM \"CSDB_CallCDRProcesseds\" WHERE \"DVPCallDirection\" = 'outbound' AND (\"SipFromUser\" = '" + sipUser + "' OR \"SipToUser\" = '" + sipUser + "') AND \"CreatedTime\"::date=date('" + qDate + "')";

    dbModel.SequelizeConn.query(query1, { type: dbModel.SequelizeConn.QueryTypes.SELECT})
        .then(function(outCallCount)
        {
            var cnt = outCallCount[0].count;
            var sum = outCallCount[0].sum;

            var tempCount = 0;
            var tempSum = 0;

            if(cnt)
            {
                tempCount = cnt;
            }

            if(sum)
            {
                tempSum = sum;
            }

            var query2 = "UPDATE \"Dashboard_DailySummaries\" SET \"TotalCount\" = '" + tempCount + "', \"TotalTime\" = '" + tempSum + "' WHERE \"SummaryDate\"::date = date('" + qDate + "') AND \"Param1\" = '" + resId + "' AND \"Param2\" = 'CALLoutbound'";

            dbModel.SequelizeConn.query(query2, { type: dbModel.SequelizeConn.QueryTypes.UPDATE})
                .then(function(updateRes)
                {
                    console.log('UPDATE RECORD SUCCESS : ' + updateRes);
                }).catch(function(err)
                {
                    console.log(err);
                });
        }).catch(function(err)
        {
            console.log(err);
        })
};

server.get('/ExecuteQuery', function (req, res, next)
{

    var startDateMoment = moment(config.StartDate);
    var endDateMoment = moment(config.EndDate);
    var daysArr = [];

    while(!(startDateMoment.isAfter(endDateMoment)))
    {
        daysArr.push(startDateMoment.format('YYYY-MM-DD'));
        startDateMoment.add(1, 'days');
    }



    User.find({company: companyId, tenant: tenantId, systemuser: true})
        .exec( function(err, users) {
            if (err)
            {
                console.log(err);
            }
            else
            {
                if (users && users.length > 0)
                {
                    users.forEach(function(user)
                    {
                        if(user && user.veeryaccount && user.veeryaccount.contact && user.resourceid)
                        {
                            var contactSplit = user.veeryaccount.contact.split('@');

                            if(contactSplit.length > 1)
                            {
                                var sipUsername = contactSplit[0];

                                var resId = user.resourceid;

                                daysArr.forEach(function(day)
                                {
                                    queryExecute(day, resId, sipUsername);
                                });


                            }

                        }

                    });

                }
                else
                {

                    console.log("no users found");

                }
            }
        });
    res.end('Service Working in Background');
    next();
});

server.listen(config.Port, function() {
    console.log('%s listening at %s', server.name, server.url);
});
