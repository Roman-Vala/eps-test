require('dotenv').config()
const axios = require('axios')
const mysql = require('mysql')



const getDevicesURL = "https://api.wattwatchers.com.au/v2/devices"
const getLongEnergyURL = "https://api.wattwatchers.com.au/v2/long-energy/"
const token = process.env.WWATCHERS_API_KEY


const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  multipleStatements: true
})

function stringifyObjValues(object) {
  return Object.keys(object)
    .reduce((previous, next) => {
      return (previous[next] = JSON.stringify(object[next]), previous)
    }, {})
}



function sendRequest(){
    const call = axios.get(getDevicesURL, {
      params: {
        
      },
      headers: {'Authorization': 'Bearer ' + token}
      
    })
    .then(handleResponse)
    .catch(handleErrors)
    
    
   }
  
function handleResponse(res){
     res.data.forEach(function(device){
        let now = Math.round((new Date()).getTime() / 1000)
        axios.get(getLongEnergyURL+device, {
            params: {
              fromTs: now - 86400,
              granularity: '15m',
              timezone: 'Australia/Sydney'
            },
            headers: {'Authorization': 'Bearer ' + token}            
          })
          .then(
            
            function(res){
              const columnString = 'eReal JSON, eRealNegative JSON, eRealPositive JSON, eReactive JSON, eReactiveNegative JSON, eReactivePositive JSON, vRMSMin JSON, vRMSMax JSON, iRMSMin JSON, iRMSMax JSON, timestamp BIGINT, duration INT'
              
              const sql = `CREATE TABLE IF NOT EXISTS ${device} (${columnString}); INSERT INTO ${device} SET ? `

                           
              res.data.forEach(function(obj, index){  
                obj = stringifyObjValues(obj)
                // obj.timestamp = new Date(obj.timestamp*1000).toISOString().slice(0, 19).replace('T', ' ')
                // console.log(obj)
                pool.query(sql, obj, function (err, result) {
                  if (err) throw err;
                  console.log(`${device} row ${index+1} inserted`)                
                  
                })
              })
            }
          )
          .catch(handleErrors)        
           
      })   
       
    }
  
  
  
  function handleErrors(err) {
      if (err.response) {
        console.log("Problem With Response ", err.response.status);
      } else if (err.request) {
        console.log("Problem With Request!")
      } else {
        console.log('Error', err.message)
      }
    }

    sendRequest()
    