const fs = require('fs');
const numeral = require('numeral');
const rimraf = require('rimraf');

const step = 5000;
const zapas = 100000;

const environments = [
    {name: 'ru', dbName: 'production',    minArchive: 3,      min: 13502128, max: 32000000},
    {name: 'uz', dbName: 'production_uz', minArchive: 21057,  min: 21057,    max: 44319},
    {name: 'ro', dbName: 'production_ro', minArchive: 297805, min: 297805,   max: 722991},
    {name: 'lt', dbName: 'production_lt', minArchive: 39855,  min: 39855,    max: 193082},
    {name: 'kz', dbName: 'production_kz', minArchive: 222110, min: 222110,   max: 1218217},
    {name: 'kg', dbName: 'production_kg', minArchive: 34136,  min: 34136,    max: 131487},
    {name: 'gb', dbName: 'production_gb', minArchive: 1,      min: 1,        max: 13524},
    {name: 'ee', dbName: 'production_ee', minArchive: 59696,  min: 59696,    max: 192595},    
]


recreateDir(`./sql`);
recreateDir(`./big_sql`);

environments.forEach(function(env) {
    
    console.log(env);

    createScripts(env, false);
    createScripts(env, true);
    

});

console.log('Ok')

function recreateDir(dir) {
    if (fs.existsSync(dir)){
        rimraf.sync(dir);
    }
    fs.mkdirSync(dir);
    
}

function getSql(dbName, minOrderId, maxOrderId, isArchive){
    const tableName = isArchive ? 'orders_checkinfo_archive' : 'orders_checkinfo';

    let sql = `USE ${dbName};
        SET SQL_SAFE_UPDATES = 0;

        UPDATE ${tableName} oc
        JOIN (
                 SELECT
                     OrderId,
                     (SELECT UUId
                      FROM ${tableName} oc_in
                      WHERE oc_in.OrderId = oc.OrderId
                             AND oc_in.CheckId <> 0
                      ORDER BY CreatedDateTimeUTC DESC
                      LIMIT 1) UUId
                 FROM
                 ${tableName} oc
                 where
                     oc.OrderId >= ${minOrderId}
                     and oc.OrderId < ${maxOrderId}
                 GROUP BY
                     OrderId
             ) maxDates
            ON oc.UUId = maxDates.UUId
        SET IsActual = 1;
        `;
    return sql;
}

function writeSql(envName, number, sql, isArchive){
    let dir = `./sql/${envName}`;
    if (!fs.existsSync(dir)){
        fs.mkdirSync(dir);
    }

    fs.writeFileSync(dir + `/${envName + (isArchive ? '_achive' : '')}_${numeral(number).format('0000')}.sql`, sql);
}

function createScripts(env, isArchive){
    let oneBigSql = '';
    let minOrderId = isArchive ? env.minArchive : env.min;
    let maxOrderId = minOrderId + step;
    let number = 0;
    while(maxOrderId <= (env.max + zapas)){
        let sql = getSql(env.dbName, minOrderId, maxOrderId, isArchive);
 
        writeSql(env.name, number, sql, isArchive);
        oneBigSql += sql;
    
        minOrderId += step;
        maxOrderId += step;
        number++;
    }
    fs.writeFileSync(`./big_sql/one_big_${env.name + (isArchive ? '_archive' : '')}.sql`, oneBigSql);
}


//     let sql = `update orders_checkinfo set IsActual = 0 where OrderId >= ${minOrderId} and OrderId < ${maxOrderId};
// `;
